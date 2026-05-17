# Must match Spark worker image (apache/spark:*-python3-ubuntu → Python 3.10). Default Airflow image uses 3.12 → PYTHON_VERSION_MISMATCH.
FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

# Pre-download Spark/Iceberg/AWS JARs agar tidak perlu Maven Central saat runtime
RUN mkdir -p /opt/spark-jars \
    && curl -fSL -o /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
    && curl -fSL -o /opt/spark-jars/hadoop-aws-3.3.4.jar \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fSL -o /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    && chmod 644 /opt/spark-jars/*.jar

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 boto3

ENV SPARK_JARS_DIR="/opt/spark-jars"
