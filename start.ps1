# One-command startup for the Amazon Data Lakehouse pipeline.
# Usage: .\start.ps1

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

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

function Invoke-SparkJob($Pattern, $Job) {
    $output = docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 $Job 2>&1
    $exitCode = $LASTEXITCODE
    $output | Select-String -Pattern $Pattern

    if ($exitCode -ne 0) {
        Write-Error "Spark job failed: $Job"
        exit $exitCode
    }
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

Write-Host ""
Write-Host "========================================"
Write-Host "  Amazon Data Lakehouse - Starting Up"
Write-Host "========================================"
Write-Host ""

# Step 1: Processing stack
Write-Host "[1/4] Starting processing stack (MinIO + Iceberg + Spark)..."
Set-Location "$Root\processing"
docker compose up -d --build
Wait-Healthy "minio"
Start-Sleep -Seconds 10
Write-Host "      Processing stack ready" -ForegroundColor Green

# Step 2: Streaming stack
Write-Host ""
Write-Host "[2/4] Starting streaming stack (Kafka + producer)..."
Set-Location "$Root\streaming"
Start-StreamingStack
Wait-Healthy "kafka"
Write-Host "      Streaming stack ready" -ForegroundColor Green

# Step 3: Orchestration stack
Write-Host ""
Write-Host "[3/4] Starting orchestration stack (Airflow)..."
Set-Location "$Root\orchestration"
docker compose up -d --build
Wait-Healthy "airflow-webserver"
Write-Host "      Orchestration stack ready" -ForegroundColor Green

# Step 4: Run the pipeline
Write-Host ""
Write-Host "[4/4] Running the full pipeline (Bronze -> Silver -> Gold -> Quality -> Bonus -> Dashboard)..."

Write-Host "      Bronze ingestion..."
Invoke-SparkJob "✓|✗|ERROR" "/jobs/bronze_ingestion.py"

Write-Host "      Silver dimensions..."
Invoke-SparkJob "✓|✗|ERROR" "/jobs/silver_dimensions.py"

Write-Host "      Gold facts..."
Invoke-SparkJob "✓|✗|ERROR" "/jobs/gold_facts.py"

Write-Host "      Data quality..."
Invoke-SparkJob "✓|✗|Results" "/jobs/data_quality.py"

Write-Host "      Great Expectations bonus validation..."
Invoke-SparkJob "✓|✗|Results|Suite|Validation" "/jobs/great_expectations_validation.py"

Write-Host "      DataHub lineage bonus..."
Invoke-SparkJob "Lineage|Jobs|Tables|MCPs|WARNING" "/jobs/emit_datahub_lineage.py"

Write-Host "      Building dashboard..."
Invoke-SparkJob "✓|Revenue|Orders" "/jobs/build_dashboard.py"

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
