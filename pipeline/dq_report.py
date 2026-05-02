"""
dq_report.json builder.

Assembles the Stage 2 DQ summary from boundary-collected metrics and the
loaded dq_rules.yaml. Writes to /data/output/dq_report.json (path overridable
via pipeline_config.yaml: output.dq_report_path).
"""

import json
import os

from pipeline.dq_rules import handling_action, issue_keys


def _pct(numerator, denominator):
    if not denominator:
        return 0.00
    return round(100.0 * numerator / denominator, 2)


def build_dq_report(
    rules,
    bronze_metrics,
    silver_metrics,
    gold_record_counts,
    orphan_count,
    run_timestamp_iso,
    execution_duration_seconds,
):
    """Assemble the dq_report dict ready for json.dump."""
    txn_raw = bronze_metrics["transactions_raw"]
    acct_raw = bronze_metrics["accounts_raw"]
    cust_raw = bronze_metrics["customers_raw"]
    txn_distinct = bronze_metrics["transactions_distinct"]

    issues = []

    # 1. duplicate_transactions
    dup_affected = bronze_metrics["duplicate_transactions"]
    if dup_affected > 0:
        # records_in_output = unique transaction_ids that survived dedup AND
        # weren't quarantined as orphans. We approximate as silver_txn_count.
        issues.append({
            "issue_type": "duplicate_transactions",
            "records_affected": dup_affected,
            "percentage_of_total": _pct(dup_affected, txn_raw),
            "handling_action": handling_action(rules, "duplicate_transactions"),
            "records_in_output": silver_metrics["silver_txn_count"],
        })

    # 2. orphaned_transactions
    if orphan_count > 0:
        issues.append({
            "issue_type": "orphaned_transactions",
            "records_affected": orphan_count,
            "percentage_of_total": _pct(orphan_count, txn_raw),
            "handling_action": handling_action(rules, "orphaned_transactions"),
            "records_in_output": 0,
        })

    # 3. amount_type_mismatch (retained subset — cast succeeded)
    tm_affected = bronze_metrics["amount_type_mismatch"]
    cast_failed = silver_metrics.get("amount_cast_failed", 0)
    if tm_affected > 0:
        # Cast-failed rows are quarantined and not present in Silver's
        # _dq_type_mismatch cohort, so silver_metrics["amount_type_mismatch"]
        # already reflects only the cast-successful subset.
        issues.append({
            "issue_type": "amount_type_mismatch",
            "records_affected": tm_affected,
            "percentage_of_total": _pct(tm_affected, txn_raw),
            "handling_action": handling_action(rules, "amount_type_mismatch"),
            "records_in_output": silver_metrics["amount_type_mismatch"],
        })

    # 3b. amount_cast_failed (TYPE_MISMATCH subset whose cast yielded NULL)
    if cast_failed > 0:
        issues.append({
            "issue_type": "amount_cast_failed",
            "records_affected": cast_failed,
            "percentage_of_total": _pct(cast_failed, txn_raw),
            "handling_action": handling_action(rules, "amount_cast_failed"),
            "records_in_output": 0,
        })

    # 4. date_format_inconsistency
    df_affected = bronze_metrics["date_format_inconsistency"]
    if df_affected > 0:
        # Denominator: transactions issues use transactions_raw; but this
        # cohort spans all three files. Use sum of raw rows across affected files.
        df_denominator = txn_raw + acct_raw + cust_raw
        issues.append({
            "issue_type": "date_format_inconsistency",
            "records_affected": df_affected,
            "percentage_of_total": _pct(df_affected, df_denominator),
            "handling_action": handling_action(rules, "date_format_inconsistency"),
            "records_in_output": df_affected,
        })

    # 5. currency_variants
    cv_affected = bronze_metrics["currency_variants"]
    if cv_affected > 0:
        issues.append({
            "issue_type": "currency_variants",
            "records_affected": cv_affected,
            "percentage_of_total": _pct(cv_affected, txn_raw),
            "handling_action": handling_action(rules, "currency_variants"),
            "records_in_output": silver_metrics["currency_variants"],
        })

    # 6. null_account_id
    np_affected = bronze_metrics["null_account_id"]
    if np_affected > 0:
        issues.append({
            "issue_type": "null_account_id",
            "records_affected": np_affected,
            "percentage_of_total": _pct(np_affected, acct_raw),
            "handling_action": handling_action(rules, "null_account_id"),
            "records_in_output": 0,
        })

    return {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_timestamp_iso,
        "stage": "2",
        "source_record_counts": {
            "accounts_raw": acct_raw,
            "transactions_raw": txn_raw,
            "customers_raw": cust_raw,
        },
        "dq_issues": issues,
        "gold_layer_record_counts": gold_record_counts,
        "execution_duration_seconds": int(execution_duration_seconds),
    }


def write_dq_report(report, path):
    """Write the report dict to disk, creating parent dirs as needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)


# Touch the imported helper so static analysers don't drop the import; this
# also makes the dependency obvious from the top of the file.
_ = issue_keys
