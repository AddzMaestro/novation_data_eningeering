"""
Schema contracts for all pipeline layers.

Source schemas pin Bronze ingestion (defeats Spark's inference heuristics on
mixed-type JSON fields like Stage 2's `amount`, `currency`, `transaction_date`).
Gold schemas match output_schema_spec.md and are enforced at write boundary only.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DateType, TimestampType, DecimalType, BooleanType
)


# ─── Source Schemas (pinned at Bronze read; all StringType — defer parsing to Silver) ────────

SOURCE_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("id_number", StringType(), nullable=True),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("dob", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("income_band", StringType(), nullable=True),
    StructField("segment", StringType(), nullable=True),
    StructField("risk_score", StringType(), nullable=True),
    StructField("kyc_status", StringType(), nullable=True),
    StructField("product_flags", StringType(), nullable=True),
])  # 12 fields

SOURCE_ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), nullable=True),
    StructField("customer_ref", StringType(), nullable=True),
    StructField("account_type", StringType(), nullable=True),
    StructField("account_status", StringType(), nullable=True),
    StructField("open_date", StringType(), nullable=True),
    StructField("product_tier", StringType(), nullable=True),
    StructField("mobile_number", StringType(), nullable=True),
    StructField("digital_channel", StringType(), nullable=True),
    StructField("credit_limit", StringType(), nullable=True),
    StructField("current_balance", StringType(), nullable=True),
    StructField("last_activity_date", StringType(), nullable=True),
])  # 11 fields

# Transactions: ALL fields read as String (incl. nested), so JSON numbers vs
# strings collapse to one type. The _raw_line column is preserved at Bronze
# so Silver can regex-detect the original JSON token type for DQ flagging
# (TYPE_MISMATCH = amount was quoted; CURRENCY_VARIANT = currency was number;
# DATE_FORMAT = transaction_date was unquoted epoch).
SOURCE_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("account_id", StringType(), nullable=True),
    StructField("transaction_date", StringType(), nullable=True),
    StructField("transaction_time", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("location", StructType([
        StructField("province", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("coordinates", StringType(), nullable=True),
    ]), nullable=True),
    StructField("metadata", StructType([
        StructField("device_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("retry_flag", StringType(), nullable=True),
    ]), nullable=True),
    StructField("merchant_subcategory", StringType(), nullable=True),
])  # 13 top-level fields


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