"""
Reusable transform functions — pure DataFrame → DataFrame.

Composed via .transform() chaining. Higher-order functions return closures.
No side effects. No .collect(). No pandas.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DecimalType, IntegerType, DateType


def deduplicate_on(key_col):
    """Dedup on key_col, keeping the row with the latest ingestion_timestamp."""
    def _dedup(df: DataFrame) -> DataFrame:
        w = Window.partitionBy(key_col).orderBy(F.col("ingestion_timestamp").desc())
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
                # Epoch integer support (Stage 2 forward-design)
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
    """Map currency variants (R, rands, 710, zar) → ZAR."""
    def _normalise(df: DataFrame) -> DataFrame:
        return df.withColumn(
            column,
            F.when(
                F.upper(F.col(column).cast(StringType())).isin("ZAR", "R", "RANDS", "710"),
                F.lit("ZAR")
            ).otherwise(F.lit("ZAR"))  # Default to ZAR — all values represent ZAR
        )
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
