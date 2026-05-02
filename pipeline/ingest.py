"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Stage 2 design notes:
  - Source schemas are pinned (StringType-only) — Spark inference would fail
    or produce inconsistent types on Stage 2's mixed JSON tokens (`amount`
    sometimes quoted, `currency` sometimes numeric, `transaction_date` mixed
    string/epoch). Type casting and DQ flagging happen in Silver.
  - For transactions we read each line as raw text, parse via from_json, and
    keep the raw line in `_raw_line`. Silver uses regex on `_raw_line` to
    recover the original JSON token type for TYPE_MISMATCH / CURRENCY_VARIANT
    / DATE_FORMAT / KEY_ABSENT detection. Single pass, no double parse.
"""

import pyspark.sql.functions as F
from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage
from pipeline.schemas import (
    SOURCE_CUSTOMERS_SCHEMA,
    SOURCE_ACCOUNTS_SCHEMA,
    SOURCE_TRANSACTIONS_SCHEMA,
)


logger = setup_logger()


def _ingest_csv(spark, path, schema, table_name, output_path, ingestion_ts):
    """Read a CSV file with a pinned schema, add ingestion_timestamp, write as Delta."""
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
        .withColumn("ingestion_timestamp", ingestion_ts)
    )
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"  {table_name}: wrote to {output_path}")


def _ingest_transactions(spark, path, output_path, ingestion_ts):
    """Read JSONL via text + from_json so we can keep `_raw_line` for downstream
    DQ-token detection (TYPE_MISMATCH / CURRENCY_VARIANT / DATE_FORMAT / KEY_ABSENT)."""
    df = (
        spark.read.text(path)
        .withColumnRenamed("value", "_raw_line")
        .withColumn("data", F.from_json(F.col("_raw_line"), SOURCE_TRANSACTIONS_SCHEMA))
        .select(F.col("data.*"), F.col("_raw_line"))
        .withColumn("ingestion_timestamp", ingestion_ts)
    )
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"  transactions: wrote to {output_path}")


def run_ingestion(config=None):
    """Execute Bronze layer ingestion."""
    if config is None:
        config = load_config()

    spark = get_or_create_spark(config)

    # Single consistent timestamp for the entire ingestion run
    ingestion_ts = F.current_timestamp()

    input_cfg = config["input"]
    bronze_path = config["output"]["bronze_path"]

    with log_stage(logger, "Bronze Ingestion"):
        _ingest_csv(
            spark,
            input_cfg["customers_path"],
            SOURCE_CUSTOMERS_SCHEMA,
            "customers",
            f"{bronze_path}/customers",
            ingestion_ts,
        )
        _ingest_csv(
            spark,
            input_cfg["accounts_path"],
            SOURCE_ACCOUNTS_SCHEMA,
            "accounts",
            f"{bronze_path}/accounts",
            ingestion_ts,
        )
        _ingest_transactions(
            spark,
            input_cfg["transactions_path"],
            f"{bronze_path}/transactions",
            ingestion_ts,
        )
