#!/bin/bash
# One-command startup for the Amazon Data Lakehouse pipeline.
# Usage: bash start.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ROOT="$(cd "$(dirname "$0")" && pwd)"

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
docker compose up -d --build

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
echo -e "${YELLOW}[4/4] Running the full pipeline (Bronze → Silver → Gold → Quality → Dashboard)...${NC}"

echo "      Bronze ingestion..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /jobs/bronze_ingestion.py 2>&1 | grep -E "✓|✗|ERROR" || true

echo "      Silver dimensions..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /jobs/silver_dimensions.py 2>&1 | grep -E "✓|✗|ERROR" || true

echo "      Gold facts..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /jobs/gold_facts.py 2>&1 | grep -E "✓|✗|ERROR" || true

echo "      Data quality..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /jobs/data_quality.py 2>&1 | grep -E "✓|✗|Results" || true

echo "      Building dashboard..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /jobs/build_dashboard.py 2>&1 | grep -E "✓|Revenue|Orders|Conversion" || true

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
echo ""
echo "  To trigger another pipeline run via Airflow:"
echo "  1. Go to http://localhost:8085"
echo "  2. Enable + trigger the 'batch_pipeline' DAG"
echo ""
