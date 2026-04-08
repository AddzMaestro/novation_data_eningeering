"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Preserves source data as-is. Adds ingestion_timestamp (single value per run).
All paths read from config YAML. No transformation at this layer.
"""

import pyspark.sql.functions as F
from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage


logger = setup_logger()


def _ingest_csv(spark, path, table_name, output_path, ingestion_ts):
    """Read a CSV file, add ingestion_timestamp, write as Delta."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("ingestion_timestamp", ingestion_ts)
    )
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"  {table_name}: wrote to {output_path}")


def _ingest_jsonl(spark, path, table_name, output_path, ingestion_ts):
    """Read a JSONL file, add ingestion_timestamp, write as Delta."""
    df = (
        spark.read
        .json(path)
        .withColumn("ingestion_timestamp", ingestion_ts)
    )
    df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"  {table_name}: wrote to {output_path}")


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
            "customers",
            f"{bronze_path}/customers",
            ingestion_ts,
        )
        _ingest_csv(
            spark,
            input_cfg["accounts_path"],
            "accounts",
            f"{bronze_path}/accounts",
            ingestion_ts,
        )
        _ingest_jsonl(
            spark,
            input_cfg["transactions_path"],
            "transactions",
            f"{bronze_path}/transactions",
            ingestion_ts,
        )
