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
docker compose up -d --build
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
Write-Host "[4/4] Running the full pipeline (Bronze -> Silver -> Gold -> Quality -> Dashboard)..."

Write-Host "      Bronze ingestion..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/bronze_ingestion.py 2>&1 | Select-String -Pattern "✓|✗|ERROR"

Write-Host "      Silver dimensions..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/silver_dimensions.py 2>&1 | Select-String -Pattern "✓|✗|ERROR"

Write-Host "      Gold facts..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/gold_facts.py 2>&1 | Select-String -Pattern "✓|✗|ERROR"

Write-Host "      Data quality..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/data_quality.py 2>&1 | Select-String -Pattern "✓|✗|Results"

Write-Host "      Building dashboard..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/build_dashboard.py 2>&1 | Select-String -Pattern "✓|Revenue|Orders"

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
Write-Host ""
