"""
Reusable transform functions — pure DataFrame → DataFrame.

Composed via .transform() chaining. Higher-order functions return closures.
No side effects. No .collect(). No pandas.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DecimalType, IntegerType, DateType


# Regex patterns — sourced from config/dq_rules.yaml; mirrored here as
# Python strings so transforms can be evaluated in Spark expressions.
RE_AMOUNT_QUOTED       = r'"amount":\s*"'
RE_DATE_NON_ISO_TXN    = r'"transaction_date":\s*([0-9]+|"[0-9]{2}/[0-9]{2}/[0-9]{4}")'
RE_CURRENCY_VARIANT    = r'"currency":\s*([0-9]+|"(R|rands|zar|RANDS)")'
RE_DATE_ISO            = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
RE_KEY_MERCHANT_SUBCAT = r'"merchant_subcategory"'


def deduplicate_on(key_col, order_col=None, ascending=True):
    """Dedup on key_col. If order_col given, keep the row with the
    earliest (ascending=True) or latest (ascending=False) value of that
    column; otherwise fall back to ingestion_timestamp DESC."""
    def _dedup(df: DataFrame) -> DataFrame:
        if order_col is not None:
            order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
        else:
            order = F.col("ingestion_timestamp").desc()
        w = Window.partitionBy(key_col).orderBy(order)
        return (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    return _dedup


def cast_date(column):
    """Cast a string column to DATE with multi-format coalesce (ISO, DD/MM/YYYY, epoch)."""
    def _cast(df: DataFrame) -> DataFrame:
        return df.withColumn(
            column,
            F.coalesce(
                F.to_date(F.col(column), "yyyy-MM-dd"),
                F.to_date(F.col(column), "dd/MM/yyyy"),
                F.when(
                    F.col(column).cast("long").isNotNull(),
                    F.to_date(F.from_unixtime(F.col(column).cast("long")))
                ),
            )
        )
    return _cast


def cast_decimal(column, precision=18, scale=2):
    """Cast a column to DECIMAL(precision, scale)."""
    def _cast(df: DataFrame) -> DataFrame:
        return df.withColumn(column, F.col(column).cast(DecimalType(precision, scale)))
    return _cast


def cast_integer(column):
    """Cast a column to IntegerType."""
    def _cast(df: DataFrame) -> DataFrame:
        return df.withColumn(column, F.col(column).cast(IntegerType()))
    return _cast


def normalise_currency(column="currency"):
    """Normalise currency variants (R, rands, zar, 710, ZAR) to canonical 'ZAR'.
    All values represent ZAR per the data dictionary."""
    def _normalise(df: DataFrame) -> DataFrame:
        return df.withColumn(column, F.lit("ZAR"))
    return _normalise


def add_column_if_missing(col_name, data_type=StringType(), default=None):
    """Add a column with a default value if it doesn't exist in the DataFrame."""
    def _add(df: DataFrame) -> DataFrame:
        if col_name not in df.columns:
            return df.withColumn(col_name, F.lit(default).cast(data_type))
        return df
    return _add


def flatten_location():
    """Flatten location.* nested fields to top-level columns."""
    def _flatten(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("province", F.col("location.province"))
            .withColumn("city", F.col("location.city"))
            .withColumn("coordinates", F.col("location.coordinates"))
            .drop("location")
        )
    return _flatten


def flatten_metadata():
    """Flatten metadata.* nested fields to top-level columns."""
    def _flatten(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("device_id", F.col("metadata.device_id"))
            .withColumn("session_id", F.col("metadata.session_id"))
            .withColumn("retry_flag", F.col("metadata.retry_flag"))
            .drop("metadata")
        )
    return _flatten


def detect_dq_signals_from_raw(raw_col="_raw_line"):
    """Add per-row boolean DQ signal columns derived from the raw JSON line.

    These signals are used both to populate the dq_flag column (priority-coalesced)
    and to count records per issue cohort in dq_report.json.
    """
    def _detect(df: DataFrame) -> DataFrame:
        raw = F.col(raw_col)
        return (
            df
            .withColumn("_dq_type_mismatch", raw.rlike(RE_AMOUNT_QUOTED))
            .withColumn("_dq_date_format", raw.rlike(RE_DATE_NON_ISO_TXN))
            .withColumn("_dq_currency_variant", raw.rlike(RE_CURRENCY_VARIANT))
        )
    return _detect


def assign_dq_flag(priority):
    """Coalesce the boolean DQ signals into a single dq_flag value using priority.

    `priority` is a list of dq_flag codes; the first matching signal wins.
    Rows with no matching signal get dq_flag = NULL.
    """
    flag_to_signal = {
        "TYPE_MISMATCH": "_dq_type_mismatch",
        "DATE_FORMAT": "_dq_date_format",
        "CURRENCY_VARIANT": "_dq_currency_variant",
    }

    def _assign(df: DataFrame) -> DataFrame:
        expr = F.lit(None).cast(StringType())
        for flag in reversed(priority):  # later items go first in nested when
            signal = flag_to_signal.get(flag)
            if signal is None or signal not in df.columns:
                continue
            expr = F.when(F.col(signal), F.lit(flag)).otherwise(expr)
        return df.withColumn("dq_flag", expr)
    return _assign
