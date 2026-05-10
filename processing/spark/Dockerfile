FROM apache/spark:3.5.1

USER root

# curl is needed to download JARs; pip for Python packages
RUN apt-get update -q && \
    apt-get install -y -q curl python3-pip && \
    rm -rf /var/lib/apt/lists/*

# ── Version pins ──────────────────────────────────────────────────────────────
ENV ICEBERG_VERSION=1.5.2
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# ── Download JARs into the Spark jars directory ───────────────────────────────
# 1. Iceberg Spark runtime (catalog + table format)
RUN curl --retry 5 -Lo /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# 2. Iceberg AWS bundle (SDK v2 classes required by S3FileIO in Iceberg 1.5+)
RUN curl --retry 5 -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# 3. Hadoop AWS connector (s3a:// filesystem → MinIO)
RUN curl --retry 5 -Lo /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

# 4. AWS SDK v1 bundle (required by hadoop-aws for S3A)
RUN curl --retry 5 -Lo /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ── Configuration ─────────────────────────────────────────────────────────────
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ── Python packages ───────────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER spark
