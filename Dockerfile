FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Fix SPARK_HOME — base image points to dist-packages but pip installs to site-packages
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# Pre-download Delta Lake JARs at build time (network available during build, not at runtime)
RUN python -c "\
from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
builder = SparkSession.builder.master('local[1]').appName('jar-download'); \
configure_spark_with_delta_pip(builder).getOrCreate().stop(); \
"

# Copy pipeline code and configuration into the image.
COPY pipeline/ pipeline/
COPY config/ config/

# Ensure pipeline package is importable
ENV PYTHONPATH=/app
# Fix Spark in --network=none Docker: bind to localhost, avoid hostname resolution
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

# Entry point — must run the complete pipeline end-to-end without interactive input.
CMD ["python", "pipeline/run_all.py"]
