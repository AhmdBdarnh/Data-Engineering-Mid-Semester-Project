#!/bin/bash
# One-command startup for the Amazon Data Lakehouse pipeline.
# Usage: bash start.sh
#
# The main batch and streaming pipelines are now run THROUGH Airflow
# (dags triggered via the Airflow CLI and polled to completion) instead of
# being run directly with spark-submit, so a normal startup exercises the
# real orchestration path.

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ROOT="$(cd "$(dirname "$0")" && pwd)"
DAG_TIMEOUT_SECONDS=1800

start_streaming_stack() {
    if docker compose up -d --build; then
        return 0
    fi

    echo "      Streaming stack start failed; waiting 25s for Kafka/Zookeeper cleanup, then retrying..."
    sleep 25
    docker compose up -d --build
}

wait_for_dag_parsed() {
    local dag_id="$1"
    echo "      Waiting for Airflow to parse DAG '$dag_id'..."
    until docker exec airflow-webserver airflow dags list --output json 2>/dev/null | grep -q "\"$dag_id\""; do
        sleep 3
    done
}

# Triggers a DAG via the Airflow CLI and polls the Airflow metadata DB
# (through the ORM, inside the container) until it reaches a terminal state.
trigger_and_wait_dag() {
    local dag_id="$1"
    local run_id="dockerstart_$(date +%s)_${dag_id}"

    docker exec airflow-webserver airflow dags trigger "$dag_id" --run-id "$run_id" >/dev/null

    echo "      Triggered '$dag_id' (run_id=$run_id) via Airflow — waiting for completion..."
    local elapsed=0
    while true; do
        state=$(docker exec airflow-webserver python3 -c "
from airflow.models import DagRun
runs = DagRun.find(dag_id='$dag_id', run_id='$run_id')
print(runs[0].state if runs else 'missing')
" 2>/dev/null | tr -d '\r')

        case "$state" in
            success)
                echo -e "      ${GREEN}✓ $dag_id succeeded${NC}"
                return 0
                ;;
            failed)
                echo "      ERROR: $dag_id FAILED — check http://localhost:8085 for task logs" >&2
                exit 1
                ;;
        esac

        sleep 10
        elapsed=$((elapsed + 10))
        if [ "$elapsed" -ge "$DAG_TIMEOUT_SECONDS" ]; then
            echo "      ERROR: $dag_id did not finish within ${DAG_TIMEOUT_SECONDS}s (last state: $state)" >&2
            exit 1
        fi
    done
}

echo ""
echo "========================================"
echo "  Amazon Data Lakehouse — Starting Up"
echo "========================================"
echo ""

# ── Step 1: Processing stack (creates the lakehouse network) ─────────────────
echo -e "${YELLOW}[1/5] Starting processing stack (MinIO + Iceberg + Spark)...${NC}"
cd "$ROOT/processing"
docker compose up -d --build

echo "      Waiting for MinIO and Spark to be healthy..."
until docker inspect minio --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 3; done
sleep 10
echo -e "${GREEN}      ✓ Processing stack ready${NC}"

# ── Step 2: Streaming stack ───────────────────────────────────────────────────
echo ""
echo -e "${YELLOW}[2/5] Starting streaming stack (Kafka + producer)...${NC}"
cd "$ROOT/streaming"
start_streaming_stack

echo "      Waiting for Kafka to be healthy..."
until docker inspect kafka --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 3; done
echo -e "${GREEN}      ✓ Streaming stack ready (producer is streaming events into Kafka)${NC}"

# ── Step 3: Orchestration stack ───────────────────────────────────────────────
echo ""
echo -e "${YELLOW}[3/5] Starting orchestration stack (Airflow)...${NC}"
cd "$ROOT/orchestration"
docker compose up -d --build

echo "      Waiting for Airflow webserver to be healthy..."
until docker inspect airflow-webserver --format='{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do sleep 5; done
echo -e "${GREEN}      ✓ Orchestration stack ready${NC}"

# ── Step 4: Run the batch pipeline THROUGH Airflow ────────────────────────────
echo ""
echo -e "${YELLOW}[4/5] Triggering the batch pipeline via Airflow (Bronze → Silver → Gold → Quality → Bonus → Dashboard)...${NC}"
wait_for_dag_parsed "batch_pipeline"
trigger_and_wait_dag "batch_pipeline"

# ── Step 5: Run the streaming pipeline THROUGH Airflow ────────────────────────
echo ""
echo -e "${YELLOW}[5/5] Triggering the streaming pipeline via Airflow (starts the Kafka consumer, builds the real-time Gold summary)...${NC}"
wait_for_dag_parsed "streaming_pipeline"
trigger_and_wait_dag "streaming_pipeline"

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
echo "  Both DAGs run on their normal schedule from now on (batch_pipeline"
echo "  daily, streaming_pipeline hourly). To re-run manually:"
echo "  docker exec airflow-webserver airflow dags trigger batch_pipeline"
echo ""
echo "  For manual/debugging spark-submit commands, see README.md."
echo ""
