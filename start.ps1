# One-command startup for the Amazon Data Lakehouse pipeline.
# Usage: .\start.ps1
#
# The main batch and streaming pipelines are now run THROUGH Airflow (dags
# triggered via the Airflow CLI and polled to completion) instead of being
# run directly with spark-submit, so a normal startup exercises the real
# orchestration path.

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$DagTimeoutSeconds = 1800

function Wait-Healthy($container) {
    Write-Host "      Waiting for $container to be healthy..." -NoNewline
    while ($true) {
        $status = docker inspect $container --format='{{.State.Health.Status}}' 2>$null
        if ($status -eq "healthy") { break }
        Start-Sleep -Seconds 3
        Write-Host "." -NoNewline
    }
    Write-Host " ready"
}

function Start-StreamingStack {
    docker compose up -d --build
    if ($LASTEXITCODE -eq 0) { return }

    Write-Host "      Streaming stack start failed; waiting 25s for Kafka/Zookeeper cleanup, then retrying..."
    Start-Sleep -Seconds 25
    docker compose up -d --build
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Streaming stack failed to start"
        exit $LASTEXITCODE
    }
}

function Wait-DagParsed($DagId) {
    Write-Host "      Waiting for Airflow to parse DAG '$DagId'..."
    while ($true) {
        $listing = docker exec airflow-webserver airflow dags list --output json 2>$null
        if ($listing -match [regex]::Escape("`"$DagId`"")) { break }
        Start-Sleep -Seconds 3
    }
}

function Invoke-DagAndWait($DagId) {
    $runId = "dockerstart_$([int][double]::Parse((Get-Date -UFormat %s)))_$DagId"
    docker exec airflow-webserver airflow dags trigger $DagId --run-id $runId | Out-Null

    Write-Host "      Triggered '$DagId' (run_id=$runId) via Airflow - waiting for completion..."
    $elapsed = 0
    while ($true) {
        $state = (docker exec airflow-webserver python3 /opt/airflow/dags/check_dag_state.py $DagId $runId 2>$null).Trim()

        if ($state -eq "success") {
            Write-Host "      $DagId succeeded" -ForegroundColor Green
            return
        }
        if ($state -eq "failed") {
            Write-Error "$DagId FAILED - check http://localhost:8085 for task logs"
            exit 1
        }

        Start-Sleep -Seconds 10
        $elapsed += 10
        if ($elapsed -ge $DagTimeoutSeconds) {
            Write-Error "$DagId did not finish within $DagTimeoutSeconds s (last state: $state)"
            exit 1
        }
    }
}

Write-Host ""
Write-Host "========================================"
Write-Host "  Amazon Data Lakehouse - Starting Up"
Write-Host "========================================"
Write-Host ""

# Step 1: Processing stack
Write-Host "[1/5] Starting processing stack (MinIO + Iceberg + Spark)..."
Set-Location "$Root\processing"
docker compose up -d --build
Wait-Healthy "minio"
Start-Sleep -Seconds 10
Write-Host "      Processing stack ready" -ForegroundColor Green

# Step 2: Streaming stack
Write-Host ""
Write-Host "[2/5] Starting streaming stack (Kafka + producer)..."
Set-Location "$Root\streaming"
Start-StreamingStack
Wait-Healthy "kafka"
Write-Host "      Streaming stack ready (producer is streaming events into Kafka)" -ForegroundColor Green

# Step 3: Orchestration stack
Write-Host ""
Write-Host "[3/5] Starting orchestration stack (Airflow)..."
Set-Location "$Root\orchestration"
docker compose up -d --build
Wait-Healthy "airflow-webserver"
Write-Host "      Orchestration stack ready" -ForegroundColor Green

# Step 4: Run the batch pipeline THROUGH Airflow
Write-Host ""
Write-Host "[4/5] Triggering the batch pipeline via Airflow (Bronze -> Silver -> Gold -> Quality -> Bonus -> Dashboard)..."
Wait-DagParsed "batch_pipeline"
Invoke-DagAndWait "batch_pipeline"

# Step 5: Run the streaming pipeline THROUGH Airflow
Write-Host ""
Write-Host "[5/5] Triggering the streaming pipeline via Airflow (starts the Kafka consumer, builds the real-time Gold summary)..."
Wait-DagParsed "streaming_pipeline"
Invoke-DagAndWait "streaming_pipeline"

Write-Host ""
Write-Host "========================================"
Write-Host "  Pipeline complete!" -ForegroundColor Green
Write-Host "========================================"
Write-Host ""
Write-Host "  Open these in your browser:"
Write-Host "  * Airflow UI : http://localhost:8085  (admin / admin)"
Write-Host "  * Spark UI   : http://localhost:8080"
Write-Host "  * MinIO      : http://localhost:9001  (minioadmin / minioadmin)"
Write-Host "  * Dashboard  : processing\jobs\dashboard.html"
Write-Host "  * GE report  : processing\jobs\great_expectations_validation_result.json"
Write-Host "  * DataHub JSON: processing\jobs\datahub_lineage.json"
Write-Host ""
Write-Host "  Both DAGs run on their normal schedule from now on (batch_pipeline"
Write-Host "  daily, streaming_pipeline hourly). To re-run manually:"
Write-Host "  docker exec airflow-webserver airflow dags trigger batch_pipeline"
Write-Host ""
Write-Host "  For manual/debugging spark-submit commands, see README.md."
Write-Host ""
