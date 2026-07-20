# Component Descriptions

This document describes every service and job in the Amazon Data Lakehouse pipeline — what it does, why it was chosen, and how it fits into the overall architecture.

---

## Infrastructure Components

### MinIO
**Role:** Object storage — the physical store for all Iceberg data and metadata files.

MinIO is an S3-compatible object store that runs locally inside Docker. Every Iceberg table's Parquet data files, delete files, and manifest files live in the `s3://warehouse/` bucket it manages. Spark reaches it via the S3A connector (`s3a://warehouse/`).

- Console: http://localhost:9001 (`minioadmin` / `minioadmin`)
- S3 API: http://localhost:9000
- Bucket layout: `warehouse/bronze/`, `warehouse/silver/`, `warehouse/gold/`, `warehouse/checkpoints/`

---

### Apache Iceberg REST Catalog
**Role:** Table catalog — maps logical table names (`demo.bronze.orders`) to physical S3 locations.

The Iceberg REST Catalog implements the Apache Iceberg REST specification. Spark resolves every table reference through it, which means all table metadata (schemas, partition specs, snapshots) is tracked centrally. This enables schema evolution, time travel queries, and ACID writes without a heavyweight metastore like Hive.

- Endpoint: http://localhost:8181
- Config key in Spark: `spark.sql.catalog.demo.uri`

---

### Apache Spark
**Role:** Distributed processing engine — runs all transformation jobs (Bronze, Silver, Gold, Quality, Dashboard).

The cluster runs as three Docker containers: one master and two workers. Jobs are submitted via `spark-submit` either manually or triggered by Airflow. Spark reads and writes Iceberg tables using the `iceberg-spark-runtime` JAR, which is bundled in the custom Docker image.

- Master UI: http://localhost:8080
- Worker 1 UI: http://localhost:8081
- Worker 2 UI: http://localhost:8082
- Spark master address: `spark://spark-master:7077`

---

### Apache Kafka
**Role:** Message broker — buffers real-time user activity events from the producer until the Spark streaming consumer reads them.

Kafka decouples the event producer from the consumer. The Python producer reads `amazon_user_activity_streaming_events.csv` and publishes each row as a JSON message to the `user_events` topic. The Spark Structured Streaming consumer reads from that topic and writes micro-batches to the `bronze.user_events_stream` Iceberg table with a 48-hour watermark for late-arrival handling.

- Broker: `kafka:9092` (internal Docker network)
- External port: `29092`
- Topic: `user_events`

---

### Apache Airflow
**Role:** Orchestration — schedules the batch and streaming pipelines as DAGs with dependency management and retry logic.

Two DAGs are defined:

| DAG | Schedule | Tasks |
|---|---|---|
| `batch_pipeline` | Daily | Bronze ingestion → Silver dimensions → Gold facts → Data quality → Great Expectations → DataHub lineage → Dashboard |
| `streaming_pipeline` | Hourly | Streaming consumer → row count check |

- Webserver: http://localhost:8085 (`admin` / `admin`)
- Metadata DB: Postgres on port 5432

---

## Pipeline Jobs

All jobs live in `processing/jobs/` and run inside the `spark-master` container via `spark-submit`.

### `bronze_ingestion.py`
Reads all five source CSV files from the `/data/` mount and writes them as Iceberg tables in the `demo.bronze` namespace. No transformations are applied — raw types and values are preserved for auditability. Adds `batch_loaded_at` or `ingestion_time` timestamps to record when the data arrived.

**Output tables:** `bronze.orders`, `bronze.product_catalog`, `bronze.product_pricing`, `bronze.reviews`, `bronze.user_events`

---

### `streaming_consumer.py`
Runs a Spark Structured Streaming job that reads JSON messages from the Kafka `user_events` topic and writes them to `bronze.user_events_stream` as an Iceberg table using `appendMode`. Applies a 48-hour event-time watermark to handle out-of-order messages without unbounded state growth. Adds `ingestion_lag_minutes` and `is_late_arrival` columns.

**Output table:** `bronze.user_events_stream`

---

### `silver_dimensions.py`
Cleans and enriches the two dimension sources:

- **`dim_product`** — drops rows with null `product_id` or `product_name`, adds `price_tier` (budget / mid / premium / luxury) and `days_on_market` derived columns.
- **`dim_product_pricing_scd`** — enforces SCD Type 2 integrity: one `is_current = true` row per product, null `effective_to` on the current row.

**Output tables:** `silver.dim_product`, `silver.dim_product_pricing_scd`

---

### `gold_facts.py`
Joins Bronze and Silver tables to build four analytics-ready tables:

- **`fact_orders`** — one row per order enriched with `category`, `brand`, and `price_tier` from `dim_product`; computes `arrival_lag_hours` and sets `late_arrival_flag` for orders where lag > 48 hours.
- **`fact_user_events`** — one row per user event with `event_date` and `ingestion_lag_minutes`.
- **`ecommerce_summary`** — daily aggregated sales + event + review metrics grouped by `category`, `brand`, and `traffic_channel`.
- **`ml_session_conversion`** — one row per session with pre-aggregated features (event counts, cart actions, session duration) and a binary `converted` label (1 if the session included a purchase click).

**Output tables:** `gold.fact_orders`, `gold.fact_user_events`, `gold.ecommerce_summary`, `gold.ml_session_conversion`

---

### `data_quality.py`
Runs 14 named checks across all three layers after the Gold build. Prints a pass/fail result for each check and exits with code 1 if any check fails, causing the Airflow DAG to stop. See [data_quality.md](data_quality.md) for the full check catalog.

---

### `great_expectations_validation.py`
Generates two Great Expectations-compatible artifacts:
- `great_expectations_suite.json` — the expectation suite (schema, row counts, value ranges).
- `great_expectations_validation_result.json` — the run result with per-expectation pass/fail statistics.

---

### `emit_datahub_lineage.py`
Writes `datahub_lineage.json` documenting dataset schemas, row counts, and job-level lineage (`CSV/Kafka → Bronze → Silver → Gold`). If the environment variable `DATAHUB_GMS_URL` is set, it also emits Metadata Change Proposals to a live DataHub instance.

---

### `build_dashboard.py`
Queries the Gold layer and generates `dashboard.html` — a self-contained HTML business dashboard with charts for revenue by category, order volume over time, top products by conversion rate, and late-arrival order analysis.

---

### `check_streaming.py`
Quick utility that prints the row count in `bronze.user_events_stream`. Used by the `streaming_pipeline` Airflow DAG to verify that the consumer is writing data.

---

### `test_connection.py`
Smoke test that verifies Spark can connect to both the Iceberg REST catalog and MinIO. Run this after starting the processing stack to confirm the environment is healthy before submitting pipeline jobs.
