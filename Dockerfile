# Use the same Spark version as before
FROM apache/spark:3.5.0

# Switch to root to install system packages
USER root

# 1. Install system dependencies for confluent-kafka
RUN apt-get update && \
    apt-get install -y gcc python3-dev librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Pre-install the Python library
RUN pip install --no-cache-dir confluent-kafka

# Switch back to the default spark user for security (optional but recommended)
# USER spark