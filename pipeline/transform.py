"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Applies: deduplication, type casting, nested field flattening, currency normalisation.
Adds forward-designed columns (merchant_subcategory, dq_flag) for Stage 2 compatibility.
All transforms are pure functions composed via .transform() chaining.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage
from pipeline.transforms import (
    deduplicate_on, cast_date, cast_decimal, cast_integer,
    normalise_currency, add_column_if_missing,
    flatten_location, flatten_metadata,
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


def _transform_accounts(spark, bronze_path, silver_path):
    """Bronze → Silver accounts: dedup, type-cast dates/decimals."""
    df = (
        spark.read.format("delta").load(f"{bronze_path}/accounts")
        .transform(deduplicate_on("account_id"))
        .transform(cast_date("open_date"))
        .transform(cast_date("last_activity_date"))
        .transform(cast_decimal("credit_limit"))
        .transform(cast_decimal("current_balance"))
    )
    df.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
    logger.info(f"  accounts: wrote to {silver_path}/accounts")


def _transform_transactions(spark, bronze_path, silver_path):
    """Bronze → Silver transactions: flatten JSON, dedup, type-cast, normalise currency."""
    df = (
        spark.read.format("delta").load(f"{bronze_path}/transactions")
        .transform(flatten_location())
        .transform(flatten_metadata())
        .transform(deduplicate_on("transaction_id"))
        .transform(cast_date("transaction_date"))
        .transform(cast_decimal("amount"))
        .transform(normalise_currency("currency"))
        # Forward-design: add columns that Stage 2 data will populate
        .transform(add_column_if_missing("merchant_subcategory", StringType(), None))
        .transform(add_column_if_missing("dq_flag", StringType(), None))
    )
    df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    logger.info(f"  transactions: wrote to {silver_path}/transactions")


def run_transformation(config=None):
    """Execute Silver layer transformation."""
    if config is None:
        config = load_config()

    spark = get_or_create_spark(config)

    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]

    with log_stage(logger, "Silver Transformation"):
        _transform_customers(spark, bronze_path, silver_path)
        _transform_accounts(spark, bronze_path, silver_path)
        _transform_transactions(spark, bronze_path, silver_path)
