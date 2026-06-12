#!/bin/bash
# One-command startup for the Amazon Data Lakehouse pipeline.
# Usage: bash start.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ROOT="$(cd "$(dirname "$0")" && pwd)"

run_spark_job() {
    local pattern="$1"
    local job="$2"

    set +e
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 "$job" 2>&1 | grep -E "$pattern"
    local spark_status=${PIPESTATUS[0]}
    set -e

    if [ "$spark_status" -ne 0 ]; then
        echo "      ERROR: Spark job failed: $job" >&2
        exit "$spark_status"
    fi
}

start_streaming_stack() {
    if docker compose up -d --build; then
        return 0
    fi

    echo "      Streaming stack start failed; waiting 25s for Kafka/Zookeeper cleanup, then retrying..."
    sleep 25
    docker compose up -d --build
}

echo ""
echo "========================================"
echo "  Amazon Data Lakehouse — Starting Up"
echo "========================================"
echo ""

# ── Step 1: Processing stack (creates the lakehouse network) ─────────────────
echo -e "${YELLOW}[1/4] Starting processing stack (MinIO + Iceberg + Spark)...${NC}"
cd "$ROOT/processing"
docker compose up -d --build

echo "      Waiting for MinIO and Spark to be healthy..."
until docker inspect minio --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 3; done
sleep 10
echo -e "${GREEN}      ✓ Processing stack ready${NC}"

# ── Step 2: Streaming stack ───────────────────────────────────────────────────
echo ""
echo -e "${YELLOW}[2/4] Starting streaming stack (Kafka + producer)...${NC}"
cd "$ROOT/streaming"
start_streaming_stack

echo "      Waiting for Kafka to be healthy..."
until docker inspect kafka --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 3; done
echo -e "${GREEN}      ✓ Streaming stack ready${NC}"

# ── Step 3: Orchestration stack ───────────────────────────────────────────────
echo ""
echo -e "${YELLOW}[3/4] Starting orchestration stack (Airflow)...${NC}"
cd "$ROOT/orchestration"
docker compose up -d --build

echo "      Waiting for Airflow webserver to be healthy..."
until docker inspect airflow-webserver --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 5; done
echo -e "${GREEN}      ✓ Orchestration stack ready${NC}"

# ── Step 4: Run the full pipeline once ───────────────────────────────────────
echo ""
echo -e "${YELLOW}[4/4] Running the full pipeline (Bronze → Silver → Gold → Quality → Bonus → Dashboard)...${NC}"

echo "      Bronze ingestion..."
run_spark_job "✓|✗|ERROR" /jobs/bronze_ingestion.py

echo "      Silver dimensions..."
run_spark_job "✓|✗|ERROR" /jobs/silver_dimensions.py

echo "      Gold facts..."
run_spark_job "✓|✗|ERROR" /jobs/gold_facts.py

echo "      Data quality..."
run_spark_job "✓|✗|Results" /jobs/data_quality.py

echo "      Great Expectations bonus validation..."
run_spark_job "✓|✗|Results|Suite|Validation" /jobs/great_expectations_validation.py

echo "      DataHub lineage bonus..."
run_spark_job "Lineage|Jobs|Tables|MCPs|WARNING" /jobs/emit_datahub_lineage.py

echo "      Building dashboard..."
run_spark_job "✓|Revenue|Orders|Conversion" /jobs/build_dashboard.py

echo ""
echo "========================================"
echo -e "${GREEN}  ✓ Pipeline complete!${NC}"
echo "========================================"
echo ""
echo "  Open these in your browser:"
echo "  • Airflow UI  : http://localhost:8085  (admin / admin)"
echo "  • Spark UI    : http://localhost:8080"
echo "  • MinIO       : http://localhost:9001  (minioadmin / minioadmin)"
echo "  • Dashboard   : processing/jobs/dashboard.html"
echo "  • GE report   : processing/jobs/great_expectations_validation_result.json"
echo "  • DataHub JSON: processing/jobs/datahub_lineage.json"
echo ""
echo "  To trigger another pipeline run via Airflow:"
echo "  1. Go to http://localhost:8085"
echo "  2. Enable + trigger the 'batch_pipeline' DAG"
echo ""
