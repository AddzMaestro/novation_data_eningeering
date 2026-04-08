"""
Structured logging for pipeline stages.

Boundary-only logging — no df.count() mid-pipeline.
Logs to stdout (Docker captures stdout).
"""

import logging
import time
from contextlib import contextmanager


def setup_logger(name="nedbank-pipeline"):
    """Configure and return a logger for the pipeline."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-5s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@contextmanager
def log_stage(logger, stage_name):
    """Context manager that logs stage start/end with elapsed time."""
    logger.info(f"[START] {stage_name}")
    start = time.time()
    try:
        yield
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"[FAIL]  {stage_name} after {elapsed:.1f}s — {e}")
        raise
    else:
        elapsed = time.time() - start
        logger.info(f"[DONE]  {stage_name} in {elapsed:.1f}s")
