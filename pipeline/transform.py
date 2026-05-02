"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Stage 2 responsibilities:
  - Dedup transactions on transaction_id (keep earliest transaction_time)
  - Quarantine orphan transactions (account_id not in accounts.csv)
  - Quarantine accounts with NULL/empty account_id
  - Detect per-row DQ signals (TYPE_MISMATCH / DATE_FORMAT / CURRENCY_VARIANT)
    from the preserved _raw_line column, assign dq_flag by priority
  - Standardise types: parse dates, cast amounts to DECIMAL(18,2),
    normalise currency variants to "ZAR"
  - Forward-compatible: missing merchant_subcategory key → NULL column

Quarantine paths:
  /data/output/silver/_quarantine/transactions_orphan/
  /data/output/silver/_quarantine/accounts_null_pk/

Output paths (written as Delta Parquet):
  /data/output/silver/customers/
  /data/output/silver/accounts/
  /data/output/silver/transactions/
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage
from pipeline.dq_rules import load_dq_rules, dq_flag_priority, quarantine_path
from pipeline.transforms import (
    deduplicate_on, cast_date, cast_decimal, cast_integer,
    normalise_currency, add_column_if_missing,
    flatten_location, flatten_metadata,
    detect_dq_signals_from_raw, assign_dq_flag,
)


logger = setup_logger()


def _transform_customers(spark, bronze_path, silver_path):
    """Bronze → Silver customers: dedup, type-cast dob/risk_score."""
    df = (
        spark.read.format("delta").load(f"{bronze_path}/customers")
        .transform(deduplicate_on("customer_id"))
        .transform(cast_date("dob"))
        .transform(cast_integer("risk_score"))
    )
    df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    logger.info(f"  customers: wrote to {silver_path}/customers")


def _transform_accounts(spark, bronze_path, silver_path, output_root, rules):
    """Bronze → Silver accounts. Quarantines NULL/empty PKs separately."""
    bronze = spark.read.format("delta").load(f"{bronze_path}/accounts")

    null_pk = bronze.filter(F.col("account_id").isNull() | (F.col("account_id") == ""))
    qpath = quarantine_path(rules, "null_account_id")
    if qpath:
        null_pk.write.format("delta").mode("overwrite").save(f"{output_root}/{qpath}")
        logger.info(f"  accounts NULL_REQUIRED quarantine: wrote to {output_root}/{qpath}")

    clean = bronze.filter(F.col("account_id").isNotNull() & (F.col("account_id") != ""))
    df = (
        clean
        .transform(deduplicate_on("account_id"))
        .transform(cast_date("open_date"))
        .transform(cast_date("last_activity_date"))
        .transform(cast_decimal("credit_limit"))
        .transform(cast_decimal("current_balance"))
    )
    df.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
    logger.info(f"  accounts: wrote to {silver_path}/accounts")


def _transform_transactions(spark, bronze_path, silver_path, output_root, rules):
    """Bronze → Silver transactions.

    Order of operations matters:
      1. Flatten location/metadata structs (preserves _raw_line)
      2. Detect raw-line DQ signals (boolean columns)
      3. Anti-join against valid accounts → quarantine orphans
      4. Dedup on transaction_id ORDER BY transaction_time ASC
      5. Type cast dates / amounts; normalise currency
      6. Assign dq_flag by priority
    """
    bronze = spark.read.format("delta").load(f"{bronze_path}/transactions")

    valid_accounts = (
        spark.read.format("delta").load(f"{silver_path}/accounts")
        .select("account_id")
        .distinct()
    )

    flagged = (
        bronze
        .transform(flatten_location())
        .transform(flatten_metadata())
        .transform(detect_dq_signals_from_raw("_raw_line"))
    )

    # Mark orphans via left-anti / left-outer split, then route accordingly.
    marker = valid_accounts.withColumn("_acct_match", F.lit(1))
    joined = flagged.join(F.broadcast(marker), on="account_id", how="left")

    orphans = joined.filter(F.col("_acct_match").isNull()).drop("_acct_match")
    qpath = quarantine_path(rules, "orphaned_transactions")
    if qpath:
        orphans.write.format("delta").mode("overwrite").save(f"{output_root}/{qpath}")
        logger.info(f"  transactions ORPHANED_ACCOUNT quarantine: wrote to {output_root}/{qpath}")

    priority = dq_flag_priority(rules)

    # Drop _raw_line BEFORE the dedup window. We only kept it through
    # detect_dq_signals_from_raw to extract the boolean DQ signals; carrying
    # ~300 bytes/row of JSON text through a row_number() window-shuffle would
    # blow the 512MB tmpfs scratch budget under the scoring system's
    # --read-only --tmpfs constraints. Quarantines above already wrote with
    # _raw_line preserved for traceability.
    cast = (
        joined.filter(F.col("_acct_match").isNotNull())
        .drop("_acct_match", "_raw_line")
        .transform(deduplicate_on("transaction_id", order_col="transaction_time", ascending=True))
        .transform(cast_date("transaction_date"))
        .transform(cast_decimal("amount"))
        .transform(normalise_currency("currency"))
        .transform(add_column_if_missing("merchant_subcategory", StringType(), None))
    )

    # Cast-failed quarantine: TYPE_MISMATCH rows whose quoted amount didn't
    # parse to a valid decimal (e.g. "ABC"). Routed out before fact-table
    # assembly so Gold never carries NULL amounts.
    cast_failed = cast.filter(F.col("amount").isNull())
    cf_qpath = quarantine_path(rules, "amount_cast_failed")
    if cf_qpath:
        cast_failed.write.format("delta").mode("overwrite").save(f"{output_root}/{cf_qpath}")
        logger.info(f"  transactions EXCLUDED_CAST_FAILED quarantine: wrote to {output_root}/{cf_qpath}")

    df = (
        cast.filter(F.col("amount").isNotNull())
        .transform(assign_dq_flag(priority))
    )
    df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    logger.info(f"  transactions: wrote to {silver_path}/transactions")


def run_transformation(config=None):
    """Execute Silver layer transformation."""
    if config is None:
        config = load_config()

    spark = get_or_create_spark(config)
    rules = load_dq_rules()

    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]
    # output_root is the parent of silver_path so quarantine paths are siblings
    output_root = silver_path.rsplit("/silver", 1)[0] if "/silver" in silver_path else silver_path

    with log_stage(logger, "Silver Transformation"):
        _transform_customers(spark, bronze_path, silver_path)
        _transform_accounts(spark, bronze_path, silver_path, output_root, rules)
        _transform_transactions(spark, bronze_path, silver_path, output_root, rules)
