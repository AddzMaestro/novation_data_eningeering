"""
Boundary-only DQ metric collector.

Computes the counts that populate /data/output/dq_report.json. Each function
runs ONE aggregation pass at a stage boundary; intermediate results are
returned as plain Python ints (small `.collect()` of a single row, not a
materialisation of millions of rows).

This module is the single place where pipeline-internal counts cross into
report-friendly values, keeping the rest of the code free of `.count()`
calls in hot paths.
"""

import pyspark.sql.functions as F

from pipeline.transforms import (
    RE_AMOUNT_QUOTED,
    RE_DATE_NON_ISO_TXN,
    RE_CURRENCY_VARIANT,
    RE_DATE_ISO,
    RE_KEY_MERCHANT_SUBCAT,
)


def _sum_when(predicate, alias):
    """Helper: SUM(IF predicate THEN 1 ELSE 0) AS alias."""
    return F.sum(F.when(predicate, 1).otherwise(0)).alias(alias)


def collect_bronze_metrics(spark, bronze_path):
    """Single-pass aggregation over Bronze tables. Returns a dict of raw
    counts the dq_report needs.

    Aggregations:
      - row counts for each source table
      - per-issue source-affected counts (transactions: type_mismatch,
        date_format, currency_variant; accounts: null_account_id, date_format;
        customers: date_format)
    """
    txn = spark.read.format("delta").load(f"{bronze_path}/transactions")
    txn_agg = txn.agg(
        F.count(F.lit(1)).alias("transactions_raw"),
        F.countDistinct(F.col("transaction_id")).alias("transactions_distinct"),
        _sum_when(F.col("_raw_line").rlike(RE_AMOUNT_QUOTED), "amount_type_mismatch"),
        _sum_when(F.col("_raw_line").rlike(RE_DATE_NON_ISO_TXN), "date_format_txn"),
        _sum_when(F.col("_raw_line").rlike(RE_CURRENCY_VARIANT), "currency_variants"),
    ).collect()[0]

    acct = spark.read.format("delta").load(f"{bronze_path}/accounts")
    acct_agg = acct.agg(
        F.count(F.lit(1)).alias("accounts_raw"),
        _sum_when(
            F.col("account_id").isNull() | (F.col("account_id") == ""),
            "null_account_id",
        ),
        _sum_when(
            F.col("open_date").isNotNull() & ~F.col("open_date").rlike(RE_DATE_ISO),
            "date_format_acct",
        ),
    ).collect()[0]

    cust = spark.read.format("delta").load(f"{bronze_path}/customers")
    cust_agg = cust.agg(
        F.count(F.lit(1)).alias("customers_raw"),
        _sum_when(
            F.col("dob").isNotNull() & ~F.col("dob").rlike(RE_DATE_ISO),
            "date_format_cust",
        ),
    ).collect()[0]

    txn_raw = int(txn_agg["transactions_raw"])
    txn_distinct = int(txn_agg["transactions_distinct"])

    return {
        "transactions_raw": txn_raw,
        "accounts_raw": int(acct_agg["accounts_raw"]),
        "customers_raw": int(cust_agg["customers_raw"]),
        # duplicate count = excess copies (rows that will be dropped by dedup)
        "duplicate_transactions": txn_raw - txn_distinct,
        "transactions_distinct": txn_distinct,
        "amount_type_mismatch": int(txn_agg["amount_type_mismatch"]),
        "date_format_inconsistency": (
            int(txn_agg["date_format_txn"])
            + int(acct_agg["date_format_acct"])
            + int(cust_agg["date_format_cust"])
        ),
        "currency_variants": int(txn_agg["currency_variants"]),
        "null_account_id": int(acct_agg["null_account_id"]),
    }


def collect_orphan_count(spark, bronze_path):
    """Anti-join count: transactions whose account_id has no match in
    accounts.csv (after null-PK exclusion)."""
    txn = (
        spark.read.format("delta").load(f"{bronze_path}/transactions")
        .select("account_id")
    )
    acct = (
        spark.read.format("delta").load(f"{bronze_path}/accounts")
        .filter(F.col("account_id").isNotNull() & (F.col("account_id") != ""))
        .select("account_id")
        .distinct()
    )
    return int(
        txn.join(acct, on="account_id", how="left_anti").count()
    )


def collect_silver_in_output_metrics(spark, silver_path):
    """Single-pass aggregation over Silver transactions to get the
    'records_in_output' counts for retain-style issues. Counts are taken
    from the Silver table because Gold drops the helper columns."""
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")
    agg = txn.agg(
        F.count(F.lit(1)).alias("silver_txn_count"),
        _sum_when(F.col("_dq_type_mismatch"), "in_amount_type_mismatch"),
        _sum_when(F.col("_dq_date_format"), "in_date_format"),
        _sum_when(F.col("_dq_currency_variant"), "in_currency_variants"),
    ).collect()[0]
    return {
        "silver_txn_count": int(agg["silver_txn_count"]),
        "amount_type_mismatch": int(agg["in_amount_type_mismatch"]),
        "currency_variants": int(agg["in_currency_variants"]),
        "date_format_transactions": int(agg["in_date_format"]),
    }


def collect_gold_record_counts(spark, gold_path):
    """Row counts for the three Gold tables — single .count() each,
    boundary-only."""
    return {
        "fact_transactions": int(spark.read.format("delta").load(f"{gold_path}/fact_transactions").count()),
        "dim_accounts": int(spark.read.format("delta").load(f"{gold_path}/dim_accounts").count()),
        "dim_customers": int(spark.read.format("delta").load(f"{gold_path}/dim_customers").count()),
    }


def collect_cast_failed_count(spark, silver_path):
    """Count of transactions diverted to the cast-failed quarantine. Returns
    0 if the table doesn't exist (no cast failures this run)."""
    qpath = f"{silver_path}/_quarantine/transactions_cast_failed"
    try:
        return int(spark.read.format("delta").load(qpath).count())
    except Exception:
        return 0
