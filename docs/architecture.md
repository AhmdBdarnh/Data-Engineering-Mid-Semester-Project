# Architecture

## Overview

The pipeline is split into three independently deployable Docker Compose stacks that all share a single Docker bridge network named **lakehouse**.

```
┌──────────────────────────────────────────────────────────┐
│  processing/docker-compose.yml                           │
│  MinIO · Iceberg REST Catalog · Spark (1 master, 2 wkr)  │
└──────────────────────────────────────────────────────────┘
         ▲                     ▲
         │  s3a://             │  REST /v1/
         │                     │
┌────────┴────────┐   ┌────────┴────────┐
│   MinIO         │   │ Iceberg REST    │
│   :9000 / :9001 │   │ Catalog :8181   │
└─────────────────┘   └─────────────────┘

┌──────────────────────────────────────────────────────────┐
│  streaming/docker-compose.yml                            │
│  Zookeeper · Kafka · Python producer                     │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  orchestration/docker-compose.yml                        │
│  Postgres · Airflow webserver + scheduler                │
└──────────────────────────────────────────────────────────┘
```

## Data Flow

```mermaid
flowchart TD
    subgraph Sources
        CSV[CSV Files\n5 datasets]
        Kafka[Kafka Topic\nuser_events]
    end

    subgraph Bronze["Bronze Layer (raw)"]
        B1[bronze.orders]
        B2[bronze.product_catalog]
        B3[bronze.product_pricing]
        B4[bronze.reviews]
        B5[bronze.user_events]
        B6[bronze.user_events_stream\n48h watermark]
    end

    subgraph Silver["Silver Layer (cleaned)"]
        S1[silver.dim_product]
        S2[silver.dim_product_pricing_scd\nSCD Type 2]
    end

    subgraph Gold["Gold Layer (aggregated)"]
        G1[gold.fact_orders]
        G2[gold.fact_user_events]
        G3[gold.ecommerce_summary]
        G4[gold.ml_session_conversion]
    end

    subgraph Orchestration
        AF[Airflow\nbatch_pipeline DAG\nstreaming_pipeline DAG]
    end

    CSV -->|bronze_ingestion.py| B1
    CSV -->|bronze_ingestion.py| B2
    CSV -->|bronze_ingestion.py| B3
    CSV -->|bronze_ingestion.py| B4
    CSV -->|bronze_ingestion.py| B5
    Kafka -->|streaming_consumer.py| B6

    B2 -->|silver_dimensions.py| S1
    B3 -->|silver_dimensions.py| S2

    B1 --> |gold_facts.py| G1
    B5 --> |gold_facts.py| G2
    S1 --> G1
    G1 --> |gold_facts.py| G3
    G2 --> |gold_facts.py| G3
    G2 --> |gold_facts.py| G4

    AF -->|schedules| B1
    AF -->|schedules| B6
```

## Services & Ports

| Service | Port | URL |
|---|---|---|
| Spark Master UI | 8080 | http://localhost:8080 |
| Spark Worker 1 UI | 8081 | http://localhost:8081 |
| Spark Worker 2 UI | 8082 | http://localhost:8082 |
| MinIO Console | 9001 | http://localhost:9001 (minioadmin / minioadmin) |
| MinIO S3 API | 9000 | http://localhost:9000 |
| Iceberg REST Catalog | 8181 | http://localhost:8181 |
| Kafka Broker | 9092 | kafka:9092 (internal) |
| Airflow Webserver | 8085 | http://localhost:8085 (admin / admin) |

## Storage Layout (MinIO)

```
s3://warehouse/
├── bronze/
│   ├── orders/
│   ├── product_catalog/
│   ├── product_pricing/
│   ├── reviews/
│   ├── user_events/
│   └── user_events_stream/
├── silver/
│   ├── dim_product/
│   └── dim_product_pricing_scd/
├── gold/
│   ├── fact_orders/
│   ├── fact_user_events/
│   ├── ecommerce_summary/
│   └── ml_session_conversion/
└── checkpoints/
    └── user_events_stream/
```

## Network

All containers join the `lakehouse` Docker bridge network.  The processing stack creates it; streaming and orchestration stacks reference it as `external: true`.  This means the processing stack must be started first.
