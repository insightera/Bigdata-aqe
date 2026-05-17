#!/bin/bash
# Download required JARs for Hive Metastore and Airflow/PySpark
set -e

LIB_DIR="$(cd "$(dirname "$0")/.." && pwd)/lib"
mkdir -p "$LIB_DIR"

echo "Downloading JARs to $LIB_DIR ..."

# PostgreSQL JDBC (for Hive Metastore)
[ -f "$LIB_DIR/postgresql-42.6.0.jar" ] || \
  curl -fSL -o "$LIB_DIR/postgresql-42.6.0.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"

# Hadoop AWS (for S3A filesystem — Hive + Spark)
[ -f "$LIB_DIR/hadoop-aws-3.3.4.jar" ] || \
  curl -fSL -o "$LIB_DIR/hadoop-aws-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

# AWS Java SDK Bundle (for S3A — Hive + Spark)
[ -f "$LIB_DIR/aws-java-sdk-bundle-1.12.262.jar" ] || \
  curl -fSL -o "$LIB_DIR/aws-java-sdk-bundle-1.12.262.jar" \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Iceberg Spark Runtime (for Spark)
[ -f "$LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" ] || \
  curl -fSL -o "$LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

echo ""
echo "Downloaded JARs:"
ls -lh "$LIB_DIR"/*.jar
echo ""
echo "Done."
