#!/usr/bin/env bash
# spark-sql dengan katalog lakehouse (Iceberg) — tanpa unduh Ivy di runtime.
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

exec docker exec "$CONTAINER" /opt/spark/bin/spark-sql \
  --conf "spark.jars.packages=" \
  --conf "spark.jars=${JARS_CSV}" \
  --conf "spark.driver.extraClassPath=/opt/spark/extra-jars/*" \
  --conf "spark.executor.extraClassPath=/opt/spark/extra-jars/*" \
  -e "$SQL"
