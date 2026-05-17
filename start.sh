#!/usr/bin/env bash
# ============================================================
#  Data Lakehouse AQE — Full Startup Script
#  Stack: Spark | Airflow | MinIO | Hive | Iceberg | Trino
#         Superset | Grafana | Prometheus | Jupyter
# ============================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

STEP=0
TOTAL=8

step() {
  STEP=$((STEP + 1))
  echo ""
  echo -e "${BLUE}[${STEP}/${TOTAL}]${NC} ${BOLD}$1${NC}"
}

wait_healthy() {
  local name="$1" max="${2:-60}" i=0
  while [ $i -lt $max ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "missing")
    case "$status" in
      healthy) echo -e "  ${GREEN}✅ $name healthy${NC}"; return 0 ;;
      unhealthy) echo -e "  ${YELLOW}⚠️  $name unhealthy (continuing)${NC}"; return 0 ;;
      missing)   echo -e "  ${RED}❌ $name not found${NC}"; return 1 ;;
    esac
    sleep 5; i=$((i + 5))
    echo -ne "  ⏳ Waiting for $name... (${i}s/${max}s)\r"
  done
  echo -e "  ${YELLOW}⚠️  $name timeout after ${max}s (continuing)${NC}"
}

print_banner() {
  echo -e "${CYAN}"
  cat << 'EOF'
  ██╗      █████╗ ██╗  ██╗███████╗██╗  ██╗ ██████╗ ██╗   ██╗███████╗███████╗
  ██║     ██╔══██╗██║ ██╔╝██╔════╝██║  ██║██╔═══██╗██║   ██║██╔════╝██╔════╝
  ██║     ███████║█████╔╝ █████╗  ███████║██║   ██║██║   ██║███████╗█████╗
  ██║     ██╔══██║██╔═██╗ ██╔══╝  ██╔══██║██║   ██║██║   ██║╚════██║██╔══╝
  ███████╗██║  ██║██║  ██╗███████╗██║  ██║╚██████╔╝╚██████╔╝███████║███████╗
  ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝
              Data Lakehouse — Adaptive Query Execution (AQE)
EOF
  echo -e "${NC}"
}

print_banner

step "Pre-flight checks"
if ! command -v docker &>/dev/null; then
  echo -e "${RED}Docker not found.${NC}"; exit 1
fi
docker compose version &>/dev/null || { echo -e "${RED}Docker Compose not found.${NC}"; exit 1; }
echo -e "  ${GREEN}✅ Docker + Compose OK${NC}"
echo -e "  ${YELLOW}Recommended: 12GB+ RAM, 20GB+ disk (profil data aqe)${NC}"

step "Downloading required JARs"
mkdir -p lib metrics
JARS=(
  "postgresql-42.6.0.jar|https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
  "hadoop-aws-3.3.4.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "aws-java-sdk-bundle-1.12.262.jar|https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  "iceberg-spark-runtime-3.5_2.12-1.5.2.jar|https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
)
for entry in "${JARS[@]}"; do
  jar="${entry%%|*}"; url="${entry##*|}"
  if [ ! -f "lib/$jar" ]; then
    echo -e "  Downloading $jar ..."
    curl -fSL -o "lib/$jar" "$url"
  else
    echo -e "  ${GREEN}✓${NC} $jar"
  fi
done

step "Building custom images (Airflow, Superset)"
docker compose build airflow-init airflow-webserver airflow-scheduler superset-init superset 2>&1 | tail -8
echo -e "  ${GREEN}✅ Images built${NC}"

step "Pulling base images"
docker compose pull 2>&1 | grep -E "Pull|complete|error" || true

step "Starting infrastructure (PostgreSQL, MinIO)"
docker compose up -d postgres minio
wait_healthy lhaqe-postgres 30
wait_healthy lhaqe-minio 30
docker compose up -d minio-init
docker exec lhaqe-postgres psql -U admin -d postgres -c "CREATE DATABASE metastore_db;" 2>/dev/null || true
docker exec lhaqe-postgres psql -U admin -d postgres -c "CREATE DATABASE iceberg_catalog;" 2>/dev/null || true
docker exec lhaqe-postgres psql -U admin -d postgres -c "CREATE DATABASE airflow_db;" 2>/dev/null || true
docker exec lhaqe-postgres psql -U admin -d postgres -c "CREATE DATABASE superset_db;" 2>/dev/null || true

step "Starting catalog layer (Hive Metastore, Iceberg REST)"
docker compose up -d hive-metastore iceberg-rest
sleep 15
wait_healthy lhaqe-iceberg-rest 40

step "Starting compute (Spark, Trino, Jupyter)"
docker compose up -d spark-master
wait_healthy lhaqe-spark-master 40
docker compose up -d spark-worker-1 spark-worker-2 trino jupyter
sleep 10

step "Starting orchestration & observability (Airflow, Superset, Prometheus, Grafana)"
docker compose up -d airflow-init
sleep 25
docker compose up -d airflow-webserver
wait_healthy lhaqe-airflow-webserver 90
docker compose up -d airflow-scheduler

docker compose up -d superset-init
sleep 30
docker compose up -d superset prometheus
wait_healthy lhaqe-prometheus 30
docker compose up -d grafana

if [ ! -f data/staging/raw_mahasiswa.csv ]; then
  echo ""
  echo -e "${YELLOW}No staging CSV found — generating default profile aqe (may take several minutes)...${NC}"
  python3 scripts/generate_bronze_data.py --mode full --profile aqe --dry-run 2>/dev/null || true
  echo -e "${YELLOW}Run: python3 scripts/generate_bronze_data.py --mode full${NC}"
fi

echo ""
echo -e "${CYAN}${BOLD}══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  SERVICE STATUS${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null | head -30 || docker compose ps

echo ""
echo -e "${CYAN}${BOLD}  SERVICE UI${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
echo -e "  Spark UI          ${GREEN}http://localhost:18080${NC}"
echo -e "  Airflow           ${GREEN}http://localhost:18681${NC}    (airflow / airflow)"
echo -e "  MinIO Console     ${GREEN}http://localhost:19001${NC}    (minioadmin / minioadmin123)"
echo -e "  Trino             ${GREEN}http://localhost:18088${NC}"
echo -e "  Superset          ${GREEN}http://localhost:18089${NC}    (admin / admin)"
echo -e "  Grafana           ${GREEN}http://localhost:13001${NC}    (admin / admin)"
echo -e "  Prometheus        ${GREEN}http://localhost:19090${NC}"
echo -e "  Jupyter           ${GREEN}http://localhost:18888${NC}    (token: lakehouse)"
echo -e "  Hive Metastore    ${GREEN}thrift://localhost:19083${NC}"
echo ""
echo -e "  ${BOLD}Trigger pipelines:${NC}"
echo -e "  ${BLUE}docker exec lhaqe-airflow-scheduler airflow dags trigger staging_to_bronze_pipeline${NC}"
echo -e "  ${BLUE}docker exec lhaqe-airflow-scheduler airflow dags trigger bronze_to_silver_pipeline --conf '{\"aqe_scenario\":\"OFF\"}'${NC}"
echo -e "  ${BLUE}docker exec lhaqe-airflow-scheduler airflow dags trigger bronze_to_silver_pipeline --conf '{\"aqe_scenario\":\"ON\"}'${NC}"
echo -e "  ${BLUE}docker exec lhaqe-airflow-scheduler airflow dags trigger silver_to_gold_pipeline${NC}"
echo ""
echo -e "${GREEN}${BOLD}Lakehouse AQE stack started.${NC}"
