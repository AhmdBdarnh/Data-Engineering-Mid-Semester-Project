# Processing Stack — Spark + Iceberg + MinIO

This is Step 1 of the final project. It sets up the data lakehouse layer.

## What's in here

| Service | Image | Purpose |
|---|---|---|
| minio | minio/minio | S3-compatible object storage (the physical data store) |
| minio-init | minio/mc | Creates the `warehouse` bucket on first boot |
| iceberg-rest | tabulario/iceberg-rest | Iceberg REST catalog (tracks table metadata) |
| spark-master | custom (apache/spark:3.5.1 + Iceberg/S3A/Kafka JARs) | Spark cluster master |
| spark-worker-1 | custom | Spark worker 1 |
| spark-worker-2 | custom | Spark worker 2 |

## Start

```bash
cd processing
docker compose up -d --build
```

The `--build` flag is needed the first time to download the Iceberg and S3A JARs into the Spark image. Subsequent starts do not need it.

## Web UIs

| UI | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8080 | — |
| Spark Worker 1 | http://localhost:8081 | — |
| Spark Worker 2 | http://localhost:8082 | — |
| Iceberg REST | http://localhost:8181/v1/config | — |

## Verify the connection

Run the test job to confirm Spark, Iceberg, and MinIO can all talk to each other:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/test_connection.py
```

Expected output:
```
✓ Namespace  demo.bronze  ready
✓ Namespace  demo.silver  ready
✓ Namespace  demo.gold    ready
✓ Iceberg table  demo.bronze.connection_test  created
✓ Test rows written to MinIO via Iceberg
✓ SUCCESS — Spark  ↔  Iceberg REST catalog  ↔  MinIO all working!
```

After running, go to the MinIO console and open the `warehouse` bucket.
You will see the Iceberg data files and metadata written to `s3://warehouse/bronze/connection_test/`.

## Run any other job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/<job_name>.py
```

Available jobs: `bronze_ingestion.py`, `silver_dimensions.py` (real SCD Type 2),
`demo_scd_change.py` (SCD proof), `gold_facts.py` (incremental order merge +
48h quarantine), `data_quality.py`, `streaming_consumer.py`,
`gold_streaming_summary.py`, `great_expectations_validation.py`,
`emit_datahub_lineage.py`, `build_dashboard.py`, `check_streaming.py`,
`test_connection.py`.

In normal operation these jobs are run through Airflow (see
`orchestration/dags/batch_pipeline.py` and `streaming_pipeline.py`), not
called directly — the commands above are for manual debugging of a single
job. See the root [`README.md`](../README.md) for the full Airflow-driven
startup flow.

## Stop

```bash
docker compose down
```

Remove stored data (full reset):

```bash
docker compose down -v
```

## Network

All services run on the `lakehouse` Docker bridge network.
The streaming and orchestration stacks will join this same network.
