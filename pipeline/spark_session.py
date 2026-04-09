"""
Shared SparkSession factory and config loader.

Single session shared across all pipeline layers — saves ~30s startup and ~500MB RAM.
Config loaded from YAML with env var override for path.
Delta JARs pre-downloaded at Docker build time, loaded via spark.jars at runtime.
"""

import os
import glob as globmod
import yaml
from pyspark.sql import SparkSession


_spark = None

# Delta JARs cached by Ivy during Docker build
_IVY_JAR_DIR = "/root/.ivy2/jars"


def load_config(config_path=None):
    """Load pipeline config from YAML. Path overridable via PIPELINE_CONFIG env var."""
    if config_path is None:
        config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def _resolve_delta_jars():
    """Find Delta Lake JARs — from Ivy cache (Docker) or via packages config (local dev)."""
    if os.path.isdir(_IVY_JAR_DIR):
        jars = globmod.glob(os.path.join(_IVY_JAR_DIR, "*.jar"))
        if jars:
            return ",".join(jars), None
    # Fallback for local dev: use Maven coordinates (requires network)
    return None, "io.delta:delta-spark_2.12:3.1.0"


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

    jar_path, packages = _resolve_delta_jars()

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
        # Parquet compression — gzip avoids Snappy native lib issues on noexec tmpfs
        .config("spark.sql.parquet.compression.codec", "gzip")
        # Reduce logging noise
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Use direct JAR paths (Docker) or Maven packages (local dev)
    if jar_path:
        builder = builder.config("spark.jars", jar_path)
    elif packages:
        builder = builder.config("spark.jars.packages", packages)

    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark


def stop_spark():
    """Stop the shared SparkSession."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
