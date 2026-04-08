"""
Shared SparkSession factory and config loader.

Single session shared across all pipeline layers — saves ~30s startup and ~500MB RAM.
Config loaded from YAML with env var override for path.
"""

import os
import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


_spark = None


def load_config(config_path=None):
    """Load pipeline config from YAML. Path overridable via PIPELINE_CONFIG env var."""
    if config_path is None:
        config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_or_create_spark(config=None):
    """Return singleton SparkSession configured for 2GB/2vCPU constraint."""
    global _spark
    if _spark is not None and not _spark._jsc.sc().isStopped():
        return _spark

    if config is None:
        config = load_config()

    spark_config = config.get("spark", {})
    master = spark_config.get("master", "local[2]")
    app_name = spark_config.get("app_name", "nedbank-de-pipeline")

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        # Memory — tight budget for 2GB container
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        # Parallelism — match 2 vCPU constraint
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        # Broadcast threshold — dims are <20MB
        .config("spark.sql.autoBroadcastJoinThreshold", "20971520")
        # Serialization — Kryo is faster and more compact
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        # AQE — adaptive query execution for better join/shuffle plans
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Delta Lake extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Reduce logging noise
        .config("spark.ui.showConsoleProgress", "false")
    )

    _spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark


def stop_spark():
    """Stop the shared SparkSession."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
