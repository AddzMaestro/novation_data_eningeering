"""
Schema contracts for all pipeline layers.

Defines exact StructTypes for Gold tables matching output_schema_spec.md.
Enforced at write boundaries only — zero extra materialisation cost.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DateType, TimestampType, DecimalType, BooleanType
)


# ─── Gold Layer Schemas (exact field order per output_schema_spec.md) ────────

GOLD_DIM_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_sk", LongType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("province", StringType(), nullable=False),
    StructField("income_band", StringType(), nullable=False),
    StructField("segment", StringType(), nullable=False),
    StructField("risk_score", IntegerType(), nullable=False),
    StructField("kyc_status", StringType(), nullable=False),
    StructField("age_band", StringType(), nullable=False),
])  # 9 fields

GOLD_DIM_ACCOUNTS_SCHEMA = StructType([
    StructField("account_sk", LongType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("account_type", StringType(), nullable=False),
    StructField("account_status", StringType(), nullable=False),
    StructField("open_date", DateType(), nullable=False),
    StructField("product_tier", StringType(), nullable=False),
    StructField("digital_channel", StringType(), nullable=False),
    StructField("credit_limit", DecimalType(18, 2), nullable=True),
    StructField("current_balance", DecimalType(18, 2), nullable=False),
    StructField("last_activity_date", DateType(), nullable=True),
])  # 11 fields

GOLD_FACT_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_sk", LongType(), nullable=False),
    StructField("transaction_id", StringType(), nullable=False),
    StructField("account_sk", LongType(), nullable=False),
    StructField("customer_sk", LongType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("transaction_timestamp", TimestampType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("merchant_subcategory", StringType(), nullable=True),
    StructField("amount", DecimalType(18, 2), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),
    StructField("province", StringType(), nullable=True),
    StructField("dq_flag", StringType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])  # 15 fields


def enforce_schema(schema):
    """Higher-order function: returns DataFrame → DataFrame that selects columns in schema order."""
    field_names = [f.name for f in schema.fields]

    def _enforce(df):
        return df.select(*field_names)

    return _enforce