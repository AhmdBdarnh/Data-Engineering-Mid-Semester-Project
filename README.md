# Amazon Data Lakehouse — Final Project

End-to-end data engineering pipeline built with **Apache Spark**, **Apache Iceberg**, **MinIO**, **Apache Kafka**, and **Apache Airflow**.

## Prerequisites

- **Docker Desktop** with Compose v2 installed and running
- **8 GB+ RAM** allocated to Docker
- Ports free: `7077`, `8080–8082`, `8085`, `8181`, `9000–9001`, `29092`

---

## Quickstart — One Command

**Linux / Mac / WSL:**
```bash
bash start.sh
```

**Windows (PowerShell):**
```powershell
.\start.ps1
```

This single script starts all three stacks in the correct order, waits for each to be healthy, then **triggers the `batch_pipeline` and `streaming_pipeline` Airflow DAGs through the Airflow CLI** (not by calling `spark-submit` directly) and polls until both finish, so the normal startup path exercises real orchestration. It prints all URLs when done. Both DAGs are unpaused by default and continue running on their normal schedule afterwards (`batch_pipeline` daily, `streaming_pipeline` hourly).

**First run takes ~10–15 minutes** (downloads Spark base image + Iceberg JARs). Subsequent runs start in under a minute.

### After the script finishes, open:

| Service | URL | Credentials |
|---|---|---|
| Airflow (DAGs) | http://localhost:8085 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Business Dashboard | `processing/jobs/dashboard.html` | open in browser |
| Great Expectations report | `processing/jobs/great_expectations_validation_result.json` | generated bonus artifact |
| DataHub lineage JSON | `processing/jobs/datahub_lineage.json` | generated bonus artifact |

---

## Architecture

Three independent Docker Compose stacks share a single Docker bridge network (`lakehouse`):

| Stack | Directory | What it runs |
|---|---|---|
| Processing | `processing/` | MinIO · Iceberg REST Catalog · Spark cluster |
| Streaming | `streaming/` | Zookeeper · Kafka · Python event producer |
| Orchestration | `orchestration/` | Postgres · Airflow webserver + scheduler |

See [`docs/architecture.md`](docs/architecture.md) for diagrams and [`docs/data_model.md`](docs/data_model.md) for full table schemas.

---

## Manual Setup (step by step)

### Step 1 — Start the processing stack

```bash
cd processing
docker compose up -d --build
```

Wait ~60 seconds for MinIO, Iceberg REST catalog, and Spark to become healthy.

Verify:
- Spark Master UI: http://localhost:8080
- MinIO Console: http://localhost:9001 (`minioadmin` / `minioadmin`)

### Step 2 — Initialise namespaces and verify connectivity

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/test_connection.py
```

Expected output: `✓ SUCCESS — Spark ↔ Iceberg REST catalog ↔ MinIO all working!`

### Step 3 — Start the streaming stack

```bash
cd ../streaming
docker compose up -d --build
```

This starts Kafka and runs the Python producer once (streams the CSV into the `user_events` topic then exits).

### Step 4 — Start the orchestration stack

```bash
cd ../orchestration
docker compose up -d --build
```

Wait ~60 seconds for the `airflow-init` container to finish setting up the database.

Airflow UI: http://localhost:8085 (`admin` / `admin`)

Both `batch_pipeline` and `streaming_pipeline` are **unpaused by default**
(`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'` in
`orchestration/docker-compose.yml`) — they run on their normal schedule
(`@daily` / `@hourly`) without any manual step in the UI.

---

## Running the Pipeline

### Option A — Through Airflow (the real orchestration path)

This is what `start.sh` / `start.ps1` do automatically. To trigger it yourself:

```bash
docker exec airflow-webserver airflow dags trigger batch_pipeline
docker exec airflow-webserver airflow dags trigger streaming_pipeline
```

Watch progress at http://localhost:8085, or poll from the CLI:

```bash
docker exec airflow-webserver airflow dags list-runs -d batch_pipeline
```

### Option B — Manual spark-submit (for debugging a single job)

Runs a job directly, bypassing Airflow — useful when iterating on one job
without waiting for the whole DAG:

```bash
# Bronze: ingest all 5 CSV files into Iceberg
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/bronze_ingestion.py

# Silver: build cleaned dimension tables (SCD Type 2)
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/silver_dimensions.py

# Gold: build fact tables and aggregates
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/gold_facts.py

# Data quality: validate all layers
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/data_quality.py

# Bonus: Great Expectations-compatible validation report
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/great_expectations_validation.py

# Bonus: DataHub lineage artifact / optional GMS emission
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/emit_datahub_lineage.py

# Dashboard: generate HTML business dashboard from Gold layer
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/build_dashboard.py
```

Then open `processing/jobs/dashboard.html` in your browser to view the business dashboard.

### Streaming consumer (long-running)

Normally started automatically by the `streaming_pipeline` DAG's
`start_streaming_consumer` task (idempotent — a `pgrep` guard prevents a
second instance from starting). To start it manually for debugging:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/streaming_consumer.py
```

Check how many rows have landed, then build the Silver-clean + Gold
real-time summary from them:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/check_streaming.py

docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/gold_streaming_summary.py
```

### SCD Type 2 change-detection proof

The source CSV is static, so a normal pipeline run alone can't show a price
actually changing. `demo_scd_change.py` proves it on demand — it takes one
real product, simulates a $5 price increase in memory, and runs it through
the same `apply_scd_merge()` function the production job uses (not part of
any DAG, safe to run anytime after `silver_dimensions.py` has run once):

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/demo_scd_change.py
```

---

## Real SCD Type 2 (dim_product_pricing_scd)

`silver_dimensions.py` performs genuine change-detection, not a copy of the
bronze table:

- **First run** (table doesn't exist): bootstraps `demo.silver.dim_product_pricing_scd`
  with the full historical version set already present in the source CSV
  (it already contains real multi-version history for many products).
- **Every later run**: only the bronze row marked `is_current = true` per
  product is treated as "today's observed price." It's compared against
  the active Silver row on `list_price`, `discount_pct`, `final_price`,
  `currency`:
  - Unchanged → left untouched.
  - Changed → the old row is closed via an Iceberg `MERGE INTO`
    (`is_current = false`, `effective_to = current_date()`), then a new
    version row is appended (`new pricing_sk`, `effective_from = today`,
    `effective_to = null`, `is_current = true`).
  - New product → appended as a new current row.
- Running the job again with unchanged input performs zero updates/inserts
  — proven by simply re-running `silver_dimensions.py` twice.
- See [`processing/jobs/demo_scd_change.py`](processing/jobs/demo_scd_change.py)
  for a reproducible proof of an actual price change (the source CSV never
  changes, so this script simulates one incoming update and shows the
  before/after: old row closed, new row inserted, exactly one current row).

## Late-Arriving Batch Orders

`gold_facts.py::build_fact_orders` computes `arrival_lag_hours =
arrival_time - event_time` for every order:

- **≤ 48 hours** (or no arrival lag available) → accepted into
  `demo.gold.fact_orders`.
- **> 48 hours** → **quarantined** into `demo.bronze.orders_quarantine`
  instead (append-only, deduplicated against existing quarantined
  `order_id`s on repeat runs) — never loaded into the fact table.
- `fact_orders` itself is no longer rebuilt from scratch each run: after an
  initial bootstrap load, every later run performs an Iceberg
  `MERGE INTO ... ON t.order_id = s.order_id`, updating a row only when the
  incoming record differs on `total_amount`, `payment_status`,
  `shipping_status`, or `quantity`, and inserting rows that don't exist yet.
  Since the source has no "last modified" timestamp, content-difference is
  used as the change signal. Re-running the pipeline with the same static
  CSV performs zero duplicate inserts.

## Streaming Path to the Business Layer

Kafka → Bronze → Silver → Gold → Dashboard, end to end:

```
Kafka topic user_events
    │  streaming_consumer.py (Structured Streaming, 48h watermark,
    │  dropDuplicates(event_id))
    ▼
demo.bronze.user_events_stream
    │  gold_streaming_summary.py — dedup by event_id
    ▼
demo.silver.user_events_stream_clean
    │  gold_streaming_summary.py — aggregate by date/type/channel/device
    ▼
demo.gold.realtime_event_summary
    │  build_dashboard.py — "Real-Time Stream Activity" section
    ▼
processing/jobs/dashboard.html
```

`realtime_event_summary` is kept **separate** from `fact_user_events` /
`ecommerce_summary`: the Kafka producer replays the same
`amazon_user_activity_streaming_events.csv` that `bronze_ingestion.py`
already loads as `demo.bronze.user_events`, so merging the two paths into
one fact table would double-count every event.

## Data Quality Checks

`processing/jobs/data_quality.py` runs after the Gold build in the Airflow batch DAG and validates all three layers (exit code 1 fails the task and the DAG on any check failure):

| Layer | Checks |
|---|---|
| Bronze | Tables are non-empty; primary keys are not null for orders, products, pricing, reviews, and user events |
| Silver | No duplicate products; SCD pricing has exactly one current row per product; no negative list prices; no duplicate `pricing_sk`; `effective_to` is consistent with `is_current` |
| Gold | Fact orders are non-empty with valid prices, no duplicate `order_id`, `arrival_time` never before `event_time`; `fact_orders`/`orders_quarantine` never share an `order_id`; late orders over 48 hours are quarantined (not merely flagged); user event IDs are not null; summary dates are not null; ML conversion labels are binary |
| Streaming (best-effort) | No duplicate `event_id` in `user_events_stream_clean`; `realtime_event_summary` is non-empty — skipped (not failed) if the streaming DAG hasn't completed a cycle yet, since it runs on an independent hourly schedule |

## Airflow Alerting

`orchestration/dags/alerting.py` provides a shared `on_failure_alert`
callback used by both DAGs:

- **Always** logs the failure (DAG id, task id, execution date, exception,
  log URL) to the task log.
- **Optionally** emails via Python's built-in `smtplib` if both
  `ALERT_SMTP_HOST` and `ALERT_EMAIL_TO` are set — no extra package
  required.
- If not configured, logs `"external alerting skipped"` and the DAG
  behaves exactly as before. Credentials are **never hardcoded** — set
  them via the shell or an `orchestration/.env` file (gitignored) before
  `docker compose up`:

```bash
# orchestration/.env  (optional; do not commit real credentials)
ALERT_EMAIL_TO=you@example.com
ALERT_SMTP_HOST=smtp.example.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=you@example.com
ALERT_SMTP_PASSWORD=your-app-password
```

To test: set the vars above, restart the orchestration stack, then fail a
task on purpose (Airflow UI → task → "Mark as failed") and check the task
log for either `alert email sent to ...` or `external alerting skipped`.

## Bonus Components

### Great Expectations

`processing/jobs/great_expectations_validation.py` writes two Great Expectations-compatible artifacts:

| Artifact | Purpose |
|---|---|
| `processing/jobs/great_expectations_suite.json` | Expectation suite for key Bronze, Silver, and Gold tables |
| `processing/jobs/great_expectations_validation_result.json` | Validation result with pass/fail statistics |

The job fails if any bonus expectation fails, so the Airflow DAG and startup script stop before the dashboard when validation is not clean.

### DataHub Lineage

`processing/jobs/emit_datahub_lineage.py` writes:

```text
processing/jobs/datahub_lineage.json
```

This file documents dataset schemas, row counts, and job-level lineage:

```text
CSV/Kafka -> Bronze -> Silver -> Gold
```

If a DataHub GMS server is available, set `DATAHUB_GMS_URL` before starting the processing stack and the job will also emit Metadata Change Proposals:

```bash
export DATAHUB_GMS_URL=http://datahub-gms:8080
bash start.sh
```

Without `DATAHUB_GMS_URL`, the project still generates the local lineage JSON and the pipeline continues normally.

---

## Verifying Iceberg Table Contents

Open a PySpark shell inside the spark-master container:

```bash
docker exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077
```

Then query any layer:

```python
# Count all tables (before/after any job, to see what changed)
for t in ["bronze.orders","bronze.product_catalog","bronze.product_pricing",
          "bronze.reviews","bronze.user_events","bronze.user_events_stream",
          "bronze.orders_quarantine",
          "silver.dim_product","silver.dim_product_pricing_scd",
          "silver.user_events_stream_clean",
          "gold.fact_orders","gold.fact_user_events",
          "gold.ecommerce_summary","gold.ml_session_conversion",
          "gold.realtime_event_summary"]:
    try:
        print(f"{t}: {spark.table(f'demo.{t}').count():,} rows")
    except Exception:
        print(f"{t}: does not exist yet")

# Inspect quarantined (late > 48h) orders — excluded from fact_orders
spark.sql("SELECT order_id, arrival_lag_hours, quarantined_at FROM demo.bronze.orders_quarantine ORDER BY arrival_lag_hours DESC LIMIT 10").show()

# Inspect SCD history for one product (multiple versions, one current)
spark.sql("""
    SELECT pricing_sk, product_id, list_price, effective_from, effective_to, is_current
    FROM demo.silver.dim_product_pricing_scd
    ORDER BY product_id, effective_from
""").show(20, truncate=False)

# Iceberg table history (time travel) — shows every snapshot created by
# each MERGE/append, proving the table is no longer fully overwritten each run
spark.sql("SELECT * FROM demo.gold.fact_orders.history").show()
spark.sql("SELECT * FROM demo.silver.dim_product_pricing_scd.history").show()
```

---

## Stopping

```bash
# Stop all stacks
cd orchestration && docker compose down
cd ../streaming && docker compose down
cd ../processing && docker compose down

# Remove all data volumes (full reset)
cd processing  && docker compose down -v
cd ../streaming  && docker compose down -v
cd ../orchestration && docker compose down -v
```

---

## Environment Variables

All optional; every one has a safe empty/default value baked into the
relevant `docker-compose.yml`.

| Variable | Where | Default | Purpose |
|---|---|---|---|
| `DATAHUB_GMS_URL` | `processing/docker-compose.yml` | empty | If set, `emit_datahub_lineage.py` also POSTs MCPs to a real DataHub GMS; otherwise it only writes the local `datahub_lineage.json` |
| `ALERT_EMAIL_TO` | `orchestration/docker-compose.yml` | empty | Recipient(s) for Airflow failure alerts (comma-separated); unset = alerting is log-only |
| `ALERT_EMAIL_FROM` | `orchestration/docker-compose.yml` | `airflow@lakehouse.local` | From address for alert emails |
| `ALERT_SMTP_HOST` | `orchestration/docker-compose.yml` | empty | SMTP server for alert emails; unset = alerting is log-only |
| `ALERT_SMTP_PORT` | `orchestration/docker-compose.yml` | `587` | SMTP port |
| `ALERT_SMTP_USER` / `ALERT_SMTP_PASSWORD` | `orchestration/docker-compose.yml` | empty | Optional SMTP auth — never hardcode a real password; set via shell or a gitignored `orchestration/.env` |

## Project Structure

```
.
├── processing/
│   ├── docker-compose.yml        # MinIO + Iceberg REST + Spark cluster
│   ├── spark/
│   │   ├── Dockerfile            # Spark + Iceberg + Kafka JARs
│   │   ├── spark-defaults.conf   # Iceberg catalog + S3A config
│   │   └── requirements.txt
│   └── jobs/
│       ├── bronze_ingestion.py   # CSV → Bronze Iceberg tables
│       ├── silver_dimensions.py  # Bronze → Silver dimensions (real SCD Type 2)
│       ├── demo_scd_change.py    # Stand-alone proof of SCD change-detection
│       ├── gold_facts.py         # Silver → Gold facts (incremental merge + 48h quarantine)
│       ├── streaming_consumer.py # Kafka → Bronze (Structured Streaming)
│       ├── gold_streaming_summary.py # Bronze stream → Silver clean → Gold real-time summary
│       ├── data_quality.py       # Quality checks across all layers
│       ├── great_expectations_validation.py # Bonus validation artifacts
│       ├── emit_datahub_lineage.py # Bonus lineage artifact / optional DataHub emission
│       ├── build_dashboard.py    # Gold layer → HTML business dashboard
│       ├── dashboard.html        # Generated dashboard (open in browser)
│       ├── check_streaming.py    # Quick streaming table row count
│       └── test_connection.py    # Connectivity smoke test
├── streaming/
│   ├── docker-compose.yml        # Zookeeper + Kafka + producer
│   └── producer/
│       ├── Dockerfile
│       ├── producer.py           # CSV → Kafka event publisher
│       └── requirements.txt
├── orchestration/
│   ├── docker-compose.yml        # Postgres + Airflow (DAGs unpaused by default)
│   ├── airflow/
│   │   └── Dockerfile            # Airflow + Docker CLI
│   └── dags/
│       ├── alerting.py           # Shared failure-alert callback (log + optional email)
│       ├── batch_pipeline.py     # Daily ETL DAG
│       └── streaming_pipeline.py # Hourly streaming DAG (consumer + Gold summary)
├── docs/
│   ├── architecture.md           # Architecture diagrams (Mermaid)
│   └── data_model.md             # Full schema + ERD (Mermaid)
├── README.md
├── README_DATA_DICTIONARY.md
├── start.sh / start.ps1          # One-command startup, triggers DAGs via Airflow CLI
├── amazon_orders_late_arrivals.csv
├── amazon_product_catalog_static_dimension.csv
├── amazon_product_pricing_scd_type2.csv
├── amazon_reviews_batch_api.csv
└── amazon_user_activity_streaming_events.csv
```

---

## Data Sources

All five datasets ship with the repo and are auto-ingested by `bronze_ingestion.py`:

| File | Description | Layer pattern |
|---|---|---|
| `amazon_orders_late_arrivals.csv` | Orders with late-arrival timestamps | Batch → Bronze |
| `amazon_product_catalog_static_dimension.csv` | Static product attributes | Batch → Bronze → Silver (dim) |
| `amazon_product_pricing_scd_type2.csv` | Price history (SCD Type 2) | Batch → Bronze → Silver (SCD-2) |
| `amazon_reviews_batch_api.csv` | Customer reviews | Batch → Bronze |
| `amazon_user_activity_streaming_events.csv` | User clickstream | Batch snapshot + Kafka stream |

## Mid-semester demo

The original mid-semester demo (SQLite + HTML dashboard, no Docker) is preserved in `midsemester_demo/`. Run it with:

```powershell
python .\midsemester_demo\build_midsemester_demo.py
```
