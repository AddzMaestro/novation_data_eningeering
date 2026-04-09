"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import sys

from pipeline.spark_session import load_config, get_or_create_spark, stop_spark
from pipeline.logger import setup_logger, log_stage
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


logger = setup_logger()


def main():
    """Run the full medallion pipeline: Bronze → Silver → Gold."""
    config = load_config()

    with log_stage(logger, "Full Pipeline"):
        get_or_create_spark(config)

        run_ingestion(config)
        run_transformation(config)
        run_provisioning(config)

    stop_spark()
    logger.info("Pipeline complete — exit 0")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
