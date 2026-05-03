"""
Stage 3 — Streaming extension.

Polls /data/stream/ for stream_*.jsonl micro-batches and incrementally upserts
two new Gold tables via Delta MERGE:

  /data/output/stream_gold/current_balances/      (one row per account_id)
  /data/output/stream_gold/recent_transactions/   (last 50 events per account)

Architecture:
  - Single SparkSession shared with the batch pipeline (no concurrency hazards).
  - State persisted to <stream_gold>/_processed_files.txt — idempotent re-runs.
  - Each file processed once: cast types -> DQ-flag -> drop orphans -> MERGE.
  - current_balances = silver/accounts.current_balance baseline + cumulative
    stream deltas (DEBIT/FEE = -amount, CREDIT/REVERSAL = +amount).
  - recent_transactions: MERGE on (account_id, transaction_id), then DELETE
    rows beyond rank 50 per affected account by transaction_timestamp DESC.
  - Loop exits cleanly after `quiesce_timeout_seconds` of no new files.

Why poll instead of inotify/watchdog: the spec (§3) explicitly accepts polling,
and it works under the scoring system's --read-only / --network=none constraints
without bringing in a new dependency.

Spec references:
  - stage3_spec_addendum.md §§3, 4, 7-9
  - stream_interface_spec.md §§3, 4, 5, 6
"""

import os
import time
import glob

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import (StructType, StructField, StringType,
                                TimestampType, DecimalType)
from delta.tables import DeltaTable

from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage
from pipeline.schemas import SOURCE_TRANSACTIONS_SCHEMA
from pipeline.dq_rules import load_dq_rules, dq_flag_priority
from pipeline.transforms import (cast_decimal, cast_date, normalise_currency,
                                 detect_dq_signals_from_raw, assign_dq_flag,
                                 flatten_location, flatten_metadata)


logger = setup_logger()


PROCESSED_FILE_NAME = "_processed_files.txt"


# -- State management --------------------------------------------------------

def _load_processed_set(stream_gold_root):
    """Load the set of already-processed filenames from disk."""
    path = os.path.join(stream_gold_root, PROCESSED_FILE_NAME)
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}


def _mark_processed(stream_gold_root, fname):
    """Append a processed filename to the on-disk state file."""
    os.makedirs(stream_gold_root, exist_ok=True)
    path = os.path.join(stream_gold_root, PROCESSED_FILE_NAME)
    with open(path, "a") as f:
        f.write(fname + "\n")


# -- Table bootstrap ---------------------------------------------------------

def _ensure_target_tables(spark, sg_root):
    """Create empty Delta tables on first run so MERGE has a target.

    Both tables are written with their final schema; subsequent merges insert
    into them. Idempotent — only creates if not already a Delta table.
    """
    cb_path = f"{sg_root}/current_balances"
    rt_path = f"{sg_root}/recent_transactions"

    cb_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("current_balance", DecimalType(18, 2), False),
        StructField("last_transaction_timestamp", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])
    rt_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("transaction_timestamp", TimestampType(), False),
        StructField("amount", DecimalType(18, 2), False),
        StructField("transaction_type", StringType(), False),
        StructField("channel", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ])

    if not DeltaTable.isDeltaTable(spark, cb_path):
        spark.createDataFrame([], cb_schema).write.format("delta").save(cb_path)
        logger.info(f"  init: created empty current_balances at {cb_path}")
    if not DeltaTable.isDeltaTable(spark, rt_path):
        spark.createDataFrame([], rt_schema).write.format("delta").save(rt_path)
        logger.info(f"  init: created empty recent_transactions at {rt_path}")


# -- Per-file processing -----------------------------------------------------

def _read_stream_file(spark, file_path):
    """Read one micro-batch with raw-line preservation.

    Same pattern as Stage 2 batch ingest — keeps the JSON token type signal
    available so DQ detection (TYPE_MISMATCH / DATE_FORMAT / CURRENCY_VARIANT)
    works on stream events too. Spec §2 says these issues "may" appear.
    """
    return (
        spark.read.text(file_path)
        .withColumnRenamed("value", "_raw_line")
        .withColumn("data", F.from_json(F.col("_raw_line"), SOURCE_TRANSACTIONS_SCHEMA))
        .select(F.col("data.*"), F.col("_raw_line"))
    )


def _enrich(df, valid_accounts_b, rules):
    """Apply Stage 2 Silver-style cleaning to one micro-batch.

    Steps:
      1. Flatten location/metadata structs.
      2. Detect DQ signals from _raw_line.
      3. Inner-join against valid accounts -> drops orphans (spec §8 permits).
      4. Cast date/decimal, normalise currency to ZAR.
      5. Build proper transaction_timestamp from date + time strings.
      6. Assign dq_flag using configured priority.
    """
    flagged = (
        df
        .transform(flatten_location())
        .transform(flatten_metadata())
        .transform(detect_dq_signals_from_raw("_raw_line"))
    )

    # Drop orphans by inner-joining with broadcast valid_accounts.
    enriched = flagged.join(F.broadcast(valid_accounts_b), on="account_id", how="inner")

    # Cast types and build timestamp before merge.
    enriched = (
        enriched
        .drop("_raw_line")
        .transform(cast_date("transaction_date"))
        .transform(cast_decimal("amount"))
        .transform(normalise_currency("currency"))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("transaction_date").cast("string"),
                    F.col("transaction_time"),
                ),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .transform(assign_dq_flag(dq_flag_priority(rules)))
    )

    return enriched


def _merge_current_balances(spark, sg_root, enriched, silver_accounts_path):
    """Apply per-account net deltas to current_balances via Delta MERGE.

    Sign convention:
      DEBIT, FEE      -> -amount  (money out)
      CREDIT, REVERSAL -> +amount (money in)

    On the first event for an account, current_balance is initialised to
    silver_accounts.current_balance + delta (silver baseline carries over).
    On subsequent events, the existing row is incremented by delta.
    """
    sign = (
        F.when(F.col("transaction_type").isin("DEBIT", "FEE"), F.lit(-1))
         .otherwise(F.lit(1))
    )
    deltas = (
        enriched
        .withColumn("delta", F.col("amount") * sign)
        .groupBy("account_id")
        .agg(
            F.sum("delta").cast(DecimalType(18, 2)).alias("delta"),
            F.max("transaction_timestamp").alias("last_transaction_timestamp"),
        )
    )

    # Pull silver baseline only for accounts in this batch (broadcast small set).
    affected_ids = deltas.select("account_id").distinct()
    silver_balance = (
        spark.read.format("delta").load(silver_accounts_path)
        .select(
            "account_id",
            F.coalesce(F.col("current_balance"), F.lit(0).cast(DecimalType(18, 2)))
                .alias("baseline_balance"),
        )
        .join(F.broadcast(affected_ids), on="account_id", how="inner")
    )

    deltas_with_base = deltas.join(silver_balance, on="account_id", how="left")

    cb = DeltaTable.forPath(spark, f"{sg_root}/current_balances")
    (
        cb.alias("t").merge(
            deltas_with_base.alias("s"),
            "t.account_id = s.account_id",
        )
        .whenMatchedUpdate(set={
            "current_balance": "t.current_balance + s.delta",
            # GREATEST guards against an older micro-batch arriving after a
            # newer one (e.g. retry, out-of-order delivery). Today's fixture
            # is filename-ordered so this never triggers, but the polling
            # contract doesn't promise strict ordering.
            "last_transaction_timestamp":
                "GREATEST(t.last_transaction_timestamp, s.last_transaction_timestamp)",
            "updated_at": "current_timestamp()",
        })
        .whenNotMatchedInsert(values={
            "account_id": "s.account_id",
            "current_balance": "COALESCE(s.baseline_balance, CAST(0 AS DECIMAL(18,2))) + s.delta",
            "last_transaction_timestamp": "s.last_transaction_timestamp",
            "updated_at": "current_timestamp()",
        })
        .execute()
    )


def _merge_recent_transactions(spark, sg_root, enriched, recent_limit):
    """Upsert recent_transactions and prune to the N most recent per account.

    Two-step pattern (Delta MERGE doesn't support per-partition LIMIT):
      1. MERGE input into target on (account_id, transaction_id).
      2. Recompute rank(transaction_timestamp DESC) per affected account; any
         row with rank > limit is deleted via a second MERGE.
    """
    rt_input = (
        enriched.select(
            "account_id", "transaction_id", "transaction_timestamp",
            "amount", "transaction_type", "channel",
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    rt = DeltaTable.forPath(spark, f"{sg_root}/recent_transactions")
    (
        rt.alias("t").merge(
            rt_input.alias("s"),
            "t.account_id = s.account_id AND t.transaction_id = s.transaction_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Trim to N most recent per affected account.
    affected = enriched.select("account_id").distinct()
    rt_df = spark.read.format("delta").load(f"{sg_root}/recent_transactions")
    rt_affected = rt_df.join(F.broadcast(affected), on="account_id", how="inner")

    w = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())
    to_delete = (
        rt_affected
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") > recent_limit)
        .select("account_id", "transaction_id")
    )

    if to_delete.head(1):
        (
            rt.alias("t").merge(
                to_delete.alias("d"),
                "t.account_id = d.account_id AND t.transaction_id = d.transaction_id",
            )
            .whenMatchedDelete()
            .execute()
        )


def _process_file(spark, file_path, valid_accounts_b, rules,
                  sg_root, silver_accounts_path, recent_limit):
    """End-to-end processing of one micro-batch file."""
    raw = _read_stream_file(spark, file_path)
    enriched = _enrich(raw, valid_accounts_b, rules).cache()
    try:
        _merge_current_balances(spark, sg_root, enriched, silver_accounts_path)
        _merge_recent_transactions(spark, sg_root, enriched, recent_limit)
    finally:
        enriched.unpersist()


# -- Main loop ---------------------------------------------------------------

def run_stream_ingestion(config=None):
    """Poll /data/stream/, process new files, exit on quiesce.

    The spec (§3) says all 12 files are present at container start, so the
    first poll iteration finds everything. The loop keeps polling until
    `quiesce_timeout_seconds` elapses without seeing a new file, then exits.
    A hard `max_runtime_seconds` cap protects against pathological cases.
    """
    if config is None:
        config = load_config()

    streaming_cfg = config.get("streaming", {})
    stream_dir = streaming_cfg.get("stream_input_path", "/data/stream")
    sg_root = streaming_cfg.get("stream_gold_path", "/data/output/stream_gold")
    poll_interval = int(streaming_cfg.get("poll_interval_seconds", 5))
    quiesce_secs = int(streaming_cfg.get("quiesce_timeout_seconds", 60))
    max_runtime = int(streaming_cfg.get("max_runtime_seconds", 1200))
    recent_limit = int(streaming_cfg.get("recent_transactions_limit", 50))

    silver_path = config["output"]["silver_path"]
    silver_accounts_path = f"{silver_path}/accounts"

    spark = get_or_create_spark(config)
    rules = load_dq_rules()

    os.makedirs(sg_root, exist_ok=True)
    _ensure_target_tables(spark, sg_root)

    # Build broadcast set of valid account_ids from Silver.
    valid_accounts = (
        spark.read.format("delta").load(silver_accounts_path)
        .select("account_id")
        .filter(F.col("account_id").isNotNull())
        .distinct()
    ).cache()
    # Trigger the cache materialisation once so subsequent broadcasts are cheap.
    valid_accounts.count()

    processed = _load_processed_set(sg_root)
    if processed:
        logger.info(f"  resume: {len(processed)} files already processed")

    last_progress = time.time()
    start = time.time()
    files_processed = 0

    with log_stage(logger, "Stream Ingestion"):
        while time.time() - start < max_runtime:
            available = sorted(glob.glob(os.path.join(stream_dir, "stream_*.jsonl")))
            new_files = [p for p in available if os.path.basename(p) not in processed]

            if new_files:
                logger.info(f"  poll: {len(new_files)} new file(s) — processing in order")
                for fp in new_files:
                    fname = os.path.basename(fp)
                    t0 = time.time()
                    _process_file(
                        spark, fp, valid_accounts, rules,
                        sg_root, silver_accounts_path, recent_limit,
                    )
                    processed.add(fname)
                    _mark_processed(sg_root, fname)
                    files_processed += 1
                    logger.info(f"    {fname}: merged in {time.time() - t0:.1f}s")
                last_progress = time.time()
            else:
                idle = time.time() - last_progress
                if idle >= quiesce_secs:
                    logger.info(
                        f"  quiesce: {idle:.0f}s without new files — exiting "
                        f"(processed {files_processed} this run)"
                    )
                    break

            time.sleep(poll_interval)

    valid_accounts.unpersist()
