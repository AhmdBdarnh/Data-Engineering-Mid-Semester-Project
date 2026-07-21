# Amazon Data Lakehouse — Documentation

End-to-end data engineering pipeline built with Apache Spark, Apache Iceberg, MinIO, Apache Kafka, and Apache Airflow.

---

## Quick Links

| Document | Description |
|---|---|
| [Setup Guide](../README.md) | Prerequisites, one-command startup, manual steps, service URLs |
| [Components](components.md) | What each service does and why it was chosen |
| [Data Quality Checks](data_quality.md) | All 20 DQ checks across Bronze, Silver, and Gold layers |
| [Architecture diagram](diagrams/architecture.jpeg) | System architecture — three Docker stacks and how they connect |
| [Data Model diagram](diagrams/data-model.png) | Full ERD for all 10 Iceberg tables |
| [Data Dictionary](../README_DATA_DICTIONARY.md) | Column-level descriptions for all source CSV files |

---

## Diagrams

All diagrams are in [`diagrams/`](diagrams/).

| File | What it shows |
|---|---|
| `architecture.jpeg` | High-level system architecture — the three Docker stacks and how they connect |
| `batch-pipeline.jpeg` | Airflow batch DAG — task order from Bronze ingestion to Dashboard |
| `streaming-pipeline.jpeg` | Airflow streaming DAG — Kafka consumer health check flow |
| `data-model.png` | Entity-relationship diagram for all Bronze, Silver, and Gold tables |
| `project-overview.png` | Full project overview — data sources, layers, outputs |
| `late-arriving.png` | How late-arriving orders are detected and flagged (48 h watermark) |
| `implementations-challenges.png` | Key implementation decisions and trade-offs |

---

## Pipeline Layers at a Glance

```
CSV Files (5 datasets)          Kafka Topic (user_events)
        │                                │
        ▼  bronze_ingestion.py           ▼  streaming_consumer.py
┌───────────────────────────────────────────────────────┐
│  BRONZE LAYER  —  raw data, as-is from source         │
│  bronze.orders · product_catalog · product_pricing    │
│  reviews · user_events · user_events_stream           │
└───────────────────────────────────────────────────────┘
        │
        ▼  silver_dimensions.py
┌───────────────────────────────────────────────────────┐
│  SILVER LAYER  —  cleaned, typed, validated           │
│  silver.dim_product · dim_product_pricing_scd (SCD2)  │
└───────────────────────────────────────────────────────┘
        │
        ▼  gold_facts.py
┌───────────────────────────────────────────────────────┐
│  GOLD LAYER  —  aggregated, analytics-ready           │
│  gold.fact_orders · fact_user_events                  │
│  ecommerce_summary · ml_session_conversion            │
└───────────────────────────────────────────────────────┘
        │
        ├─▶ build_dashboard.py        →  dashboard.html
        ├─▶ great_expectations_validation.py  →  validation_result.json
        └─▶ emit_datahub_lineage.py   →  datahub_lineage.json
```

---

## Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Storage | MinIO (S3-compatible) | Stores all Iceberg data files and metadata |
| Table format | Apache Iceberg | ACID transactions, schema evolution, time travel |
| Catalog | Iceberg REST Catalog | Registers and resolves Iceberg table locations |
| Processing | Apache Spark 3.x | Distributed transformation engine |
| Streaming | Apache Kafka | Event stream for real-time user activity |
| Orchestration | Apache Airflow | DAG scheduling and pipeline monitoring |
| Validation | Great Expectations | Expectation suites and validation reports |
| Lineage | DataHub (JSON mode) | Dataset-level job lineage artifact |
