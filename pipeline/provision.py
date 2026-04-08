"""
Gold layer: Join and aggregate Silver tables into the scored Kimball star schema.

Produces:
  - dim_customers  (9 fields)  — with derived age_band from dob
  - dim_accounts   (11 fields) — customer_ref renamed to customer_id
  - fact_transactions (15 fields) — surrogate keys resolved via broadcast joins

Surrogate keys: row_number() ORDER BY natural_key — deterministic, readable, idempotent.
Dim tables cached (consumed twice: write + fact join), then unpersisted.
"""

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

from pipeline.spark_session import get_or_create_spark, load_config
from pipeline.logger import setup_logger, log_stage
from pipeline.schemas import (
    enforce_schema,
    GOLD_DIM_CUSTOMERS_SCHEMA,
    GOLD_DIM_ACCOUNTS_SCHEMA,
    GOLD_FACT_TRANSACTIONS_SCHEMA,
)


logger = setup_logger()


def _derive_age_band(df):
    """Derive age_band from dob. Buckets: 18-25, 26-35, 36-45, 46-55, 56-65, 65+."""
    return df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25)
    ).withColumn(
        "age_band",
        F.when(F.col("age") >= 65, F.lit("65+"))
        .when(F.col("age") >= 56, F.lit("56-65"))
        .when(F.col("age") >= 46, F.lit("46-55"))
        .when(F.col("age") >= 36, F.lit("36-45"))
        .when(F.col("age") >= 26, F.lit("26-35"))
        .when(F.col("age") >= 18, F.lit("18-25"))
        .otherwise(F.lit(None))
    ).drop("age")


def _add_surrogate_key(df, sk_name, natural_key):
    """Add a surrogate key column using row_number() ordered by natural key."""
    w = Window.orderBy(natural_key)
    return df.withColumn(sk_name, F.row_number().over(w).cast("bigint"))


def _build_dim_customers(spark, silver_path, gold_path):
    """Build dim_customers: 9 fields with derived age_band, no raw dob."""
    df = (
        spark.read.format("delta").load(f"{silver_path}/customers")
        .transform(_derive_age_band)
        .transform(lambda d: _add_surrogate_key(d, "customer_sk", "customer_id"))
        .transform(enforce_schema(GOLD_DIM_CUSTOMERS_SCHEMA))
    )
    # Cache — consumed twice: write + fact join lookup
    df.cache()
    df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")
    logger.info(f"  dim_customers: wrote to {gold_path}/dim_customers")
    return df


def _build_dim_accounts(spark, silver_path, gold_path):
    """Build dim_accounts: 11 fields, customer_ref → customer_id."""
    df = (
        spark.read.format("delta").load(f"{silver_path}/accounts")
        .withColumnRenamed("customer_ref", "customer_id")
        .transform(lambda d: _add_surrogate_key(d, "account_sk", "account_id"))
        .withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
        .transform(enforce_schema(GOLD_DIM_ACCOUNTS_SCHEMA))
    )
    # Cache — consumed twice: write + fact join lookup
    df.cache()
    df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_accounts")
    logger.info(f"  dim_accounts: wrote to {gold_path}/dim_accounts")
    return df


def _build_fact_transactions(spark, silver_path, gold_path, dim_accounts, dim_customers):
    """Build fact_transactions: 15 fields with resolved surrogate keys."""
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")

    # Prepare lookup DataFrames for broadcast join
    acct_lookup = dim_accounts.select(
        F.col("account_id"),
        F.col("account_sk"),
        F.col("customer_id").alias("_acct_customer_id"),
    )
    cust_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_customer_id"),
        F.col("customer_sk"),
    )

    df = (
        txn
        # Resolve account_sk via broadcast join on account_id
        .join(F.broadcast(acct_lookup), on="account_id", how="inner")
        # Resolve customer_sk via broadcast join on customer_id
        .join(
            F.broadcast(cust_lookup),
            on=(F.col("_acct_customer_id") == F.col("_cust_customer_id")),
            how="inner",
        )
        .drop("_acct_customer_id", "_cust_customer_id")
        # Combine date + time → timestamp
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("transaction_date").cast("string"), F.col("transaction_time"))
            )
        )
        # Surrogate key
        .transform(lambda d: _add_surrogate_key(d, "transaction_sk", "transaction_id"))
        # Enforce exact schema order (15 fields)
        .transform(enforce_schema(GOLD_FACT_TRANSACTIONS_SCHEMA))
    )
    df.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_transactions")
    logger.info(f"  fact_transactions: wrote to {gold_path}/fact_transactions")


def run_provisioning(config=None):
    """Execute Gold layer provisioning."""
    if config is None:
        config = load_config()

    spark = get_or_create_spark(config)

    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]

    with log_stage(logger, "Gold Provisioning"):
        dim_customers = _build_dim_customers(spark, silver_path, gold_path)
        dim_accounts = _build_dim_accounts(spark, silver_path, gold_path)
        _build_fact_transactions(spark, silver_path, gold_path, dim_accounts, dim_customers)

        # Release cache
        dim_customers.unpersist()
        dim_accounts.unpersist()
