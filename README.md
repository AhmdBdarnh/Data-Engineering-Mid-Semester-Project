# Amazon Data Lakehouse — Final Project

End-to-end data engineering pipeline built with **Apache Spark**, **Apache Iceberg**, **MinIO**, **Apache Kafka**, and **Apache Airflow**.

## Architecture

Three independent Docker Compose stacks share a single Docker bridge network (`lakehouse`):

| Stack | Directory | What it runs |
|---|---|---|
| Processing | `processing/` | MinIO · Iceberg REST Catalog · Spark cluster |
| Streaming | `streaming/` | Zookeeper · Kafka · Python event producer |
| Orchestration | `orchestration/` | Postgres · Airflow webserver + scheduler |

See [`docs/architecture.md`](docs/architecture.md) for diagrams and [`docs/data_model.md`](docs/data_model.md) for full table schemas.

---

## Prerequisites

- **Docker Desktop** (with Compose v2)
- **8 GB+ RAM** allocated to Docker
- Ports free: `7077`, `8080–8082`, `8085`, `8181`, `9000–9001`, `9092`

---

## Quick Start

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

---

## Running the Pipeline

### Option A — Manual spark-submit (for testing)

Run each layer individually:

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
```

### Option B — Airflow DAGs (scheduled)

1. Open http://localhost:8085 and log in as `admin` / `admin`
2. Enable the **`batch_pipeline`** DAG — runs Bronze → Silver → Gold → Data Quality daily
3. Enable the **`streaming_pipeline`** DAG — starts the Kafka consumer and verifies rows hourly
4. Trigger a manual run with the ▶ button on either DAG

### Streaming consumer (long-running)

The streaming consumer reads from Kafka and writes to `demo.bronze.user_events_stream` (Iceberg) continuously:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/streaming_consumer.py
```

Check how many rows have landed:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/check_streaming.py
```

---

## Verifying Iceberg Table Contents

Open a PySpark shell inside the spark-master container:

```bash
docker exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077
```

Then query any layer:

```python
# Count all tables
for t in ["bronze.orders","bronze.product_catalog","bronze.product_pricing",
          "bronze.reviews","bronze.user_events","bronze.user_events_stream",
          "silver.dim_product","silver.dim_product_pricing_scd",
          "gold.fact_orders","gold.fact_user_events",
          "gold.ecommerce_summary","gold.ml_session_conversion"]:
    print(f"{t}: {spark.table(f'demo.{t}').count():,} rows")

# Inspect late-arriving orders
spark.sql("""
    SELECT order_id, arrival_lag_hours, late_arrival_flag
    FROM demo.gold.fact_orders
    WHERE arrival_lag_hours > 48
    ORDER BY arrival_lag_hours DESC
    LIMIT 10
""").show()

# Iceberg table history (time travel)
spark.sql("SELECT * FROM demo.gold.fact_orders.history").show()
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
│       ├── silver_dimensions.py  # Bronze → Silver dimensions (SCD-2)
│       ├── gold_facts.py         # Silver → Gold facts + aggregates
│       ├── streaming_consumer.py # Kafka → Bronze (Structured Streaming)
│       ├── data_quality.py       # Quality checks across all layers
│       ├── check_streaming.py    # Quick streaming table row count
│       └── test_connection.py    # Connectivity smoke test
├── streaming/
│   ├── docker-compose.yml        # Zookeeper + Kafka + producer
│   └── producer/
│       ├── Dockerfile
│       ├── producer.py           # CSV → Kafka event publisher
│       └── requirements.txt
├── orchestration/
│   ├── docker-compose.yml        # Postgres + Airflow
│   ├── airflow/
│   │   └── Dockerfile            # Airflow + Docker CLI
│   └── dags/
│       ├── batch_pipeline.py     # Daily ETL DAG
│       └── streaming_pipeline.py # Hourly streaming health DAG
├── docs/
│   ├── architecture.md           # Architecture diagrams (Mermaid)
│   └── data_model.md             # Full schema + ERD (Mermaid)
├── README.md
├── README_DATA_DICTIONARY.md
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
