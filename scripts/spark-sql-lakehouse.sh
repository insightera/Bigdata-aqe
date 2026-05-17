#!/usr/bin/env bash
# spark-sql dengan katalog lakehouse (Iceberg) — tanpa Ivy, tanpa event log S3A di CLI.
set -euo pipefail

CONTAINER="${SPARK_CONTAINER:-lhaqe-spark-master}"
SQL="${*:-SHOW TABLES IN lakehouse.gold}"

JARS=(
  "/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
  "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar"
  "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"
)
JARS_CSV=$(IFS=,; echo "${JARS[*]}")

if ! docker exec "$CONTAINER" test -f "${JARS[0]}"; then
  echo "ERROR: JAR Iceberg tidak ada di container." >&2
  echo "  Jalankan di host: ./scripts/download-jars.sh" >&2
  echo "  Lalu restart Spark: docker compose up -d spark-master spark-worker-1 spark-worker-2" >&2
  exit 1
fi

# CLI ad-hoc: matikan event log (hindari error S3A path) + driver lokal di container master
exec docker exec "$CONTAINER" /opt/spark/bin/spark-sql \
  --conf "spark.jars.packages=" \
  --conf "spark.jars=${JARS_CSV}" \
  --conf "spark.driver.extraClassPath=/opt/spark/extra-jars/*" \
  --conf "spark.executor.extraClassPath=/opt/spark/extra-jars/*" \
  --conf "spark.eventLog.enabled=false" \
  --conf "spark.master=local[*]" \
  --conf "spark.hadoop.fs.s3a.metadatastore.impl=org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore" \
  --conf "spark.sql.catalog.lakehouse"=org.apache.iceberg.spark.SparkCatalog \
  --conf "spark.sql.catalog.lakehouse.type"=hive \
  --conf "spark.sql.catalog.lakehouse.uri"=thrift://hive-metastore:9083 \
  --conf "spark.sql.catalog.lakehouse.warehouse"=s3a://warehouse/ \
  --conf "spark.sql.defaultCatalog"=lakehouse \
  --conf "spark.hadoop.fs.s3a.endpoint"=http://minio:9000 \
  --conf "spark.hadoop.fs.s3a.access.key"=minioadmin \
  --conf "spark.hadoop.fs.s3a.secret.key"=minioadmin123 \
  --conf "spark.hadoop.fs.s3a.path.style.access"=true \
  --conf "spark.hadoop.fs.s3a.impl"=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled"=false \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider"=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf "spark.sql.extensions"=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "$SQL"
