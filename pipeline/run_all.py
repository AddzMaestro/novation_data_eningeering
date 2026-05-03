"""
Pipeline entry point.

Stage 2 batch orchestration:
  1. Ingest    — raw → Bronze Delta
  2. Collect bronze metrics (single agg pass; feeds dq_report.json)
  3. Transform — Bronze → Silver Delta + quarantine tables
  4. Collect silver metrics (records_in_output per issue cohort)
  5. Provision — Silver → Gold Delta
  6. Collect gold record counts + write /data/output/dq_report.json

Stage 3 streaming extension (runs after batch in the same container):
  7. Stream ingest — poll /data/stream/, MERGE into stream_gold/

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import sys
import time
from datetime import datetime, timezone

from pipeline.spark_session import load_config, get_or_create_spark, stop_spark
from pipeline.logger import setup_logger, log_stage
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.dq_rules import load_dq_rules
from pipeline.dq_metrics import (
    collect_bronze_metrics,
    collect_orphan_count,
    collect_silver_in_output_metrics,
    collect_gold_record_counts,
    collect_cast_failed_count,
)
from pipeline.dq_report import build_dq_report, write_dq_report
from pipeline.stream_ingest import run_stream_ingestion


logger = setup_logger()


def main():
    """Run the full medallion pipeline + DQ reporting."""
    config = load_config()
    rules = load_dq_rules()

    run_start = time.time()
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]
    dq_report_path = config["output"].get("dq_report_path", "/data/output/dq_report.json")

    with log_stage(logger, "Full Pipeline"):
        spark = get_or_create_spark(config)

        run_ingestion(config)

        with log_stage(logger, "Collect Bronze metrics"):
            bronze_metrics = collect_bronze_metrics(spark, bronze_path)
            orphan_count = collect_orphan_count(spark, bronze_path)
            logger.info(
                f"  bronze: txn={bronze_metrics['transactions_raw']} "
                f"acct={bronze_metrics['accounts_raw']} "
                f"cust={bronze_metrics['customers_raw']} "
                f"orphan={orphan_count}"
            )

        run_transformation(config)

        with log_stage(logger, "Collect Silver metrics"):
            silver_metrics = collect_silver_in_output_metrics(spark, silver_path)
            cast_failed_count = collect_cast_failed_count(spark, silver_path)
            silver_metrics["amount_cast_failed"] = cast_failed_count
            logger.info(
                f"  silver: txn={silver_metrics['silver_txn_count']} "
                f"cast_failed={cast_failed_count}"
            )

        run_provisioning(config)

        with log_stage(logger, "Collect Gold metrics + write dq_report.json"):
            gold_counts = collect_gold_record_counts(spark, gold_path)
            duration = time.time() - run_start
            report = build_dq_report(
                rules=rules,
                bronze_metrics=bronze_metrics,
                silver_metrics=silver_metrics,
                gold_record_counts=gold_counts,
                orphan_count=orphan_count,
                run_timestamp_iso=run_timestamp,
                execution_duration_seconds=duration,
            )
            write_dq_report(report, dq_report_path)
            logger.info(f"  dq_report: wrote to {dq_report_path}")

        # Stage 3 — streaming extension (only runs if streaming config present).
        # Reuses the same SparkSession the batch stages built. Quiesces on its
        # own when no new files arrive for `quiesce_timeout_seconds`.
        if "streaming" in config:
            run_stream_ingestion(config)

    stop_spark()
    logger.info("Pipeline complete — exit 0")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
