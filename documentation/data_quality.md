# Data Quality Checks

All quality checks are implemented in `processing/jobs/data_quality.py` and run automatically after the Gold build in the `batch_pipeline` Airflow DAG.

---

## How to Run

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/data_quality.py
```

**Exit codes:**
- `0` — all checks passed
- `1` — one or more checks failed (Airflow marks the task as failed and stops the DAG)

**Sample output:**
```
============================================================
  Data Quality Checks
============================================================

  --------------------------------------------------
  Bronze layer
  --------------------------------------------------
  ✓ [PASS] DQ-B1a row-count (demo.bronze.orders)   rows = 5,000
  ✓ [PASS] DQ-B1b null-pk  (demo.bronze.orders)    null order_id = 0
  ...

============================================================
  Results: 14/14 passed  |  0 failed
============================================================
```

---

## Bronze Layer Checks

The Bronze checks validate that raw ingestion completed successfully and that primary keys are intact. No transformations are applied at this layer — if a check fails here, the source data or ingestion job has a problem.

| Check ID | Table | What is validated | Pass condition |
|---|---|---|---|
| DQ-B1a | `bronze.orders` | Table is non-empty | row count > 0 |
| DQ-B1b | `bronze.orders` | Primary key integrity | no NULL `order_id` |
| DQ-B2a | `bronze.product_catalog` | Table is non-empty | row count > 0 |
| DQ-B2b | `bronze.product_catalog` | Primary key integrity | no NULL `product_id` |
| DQ-B3a | `bronze.product_pricing` | Table is non-empty | row count > 0 |
| DQ-B3b | `bronze.product_pricing` | Primary key integrity | no NULL `pricing_sk` |
| DQ-B4a | `bronze.reviews` | Table is non-empty | row count > 0 |
| DQ-B4b | `bronze.reviews` | Primary key integrity | no NULL `review_id` |
| DQ-B5a | `bronze.user_events` | Table is non-empty | row count > 0 |
| DQ-B5b | `bronze.user_events` | Primary key integrity | no NULL `event_id` |

---

## Silver Layer Checks

The Silver checks validate the business rules applied during dimension cleaning. A failure here means the Silver transformation job produced invalid output.

| Check ID | Table | What is validated | Pass condition |
|---|---|---|---|
| DQ-S1 | `silver.dim_product` | No duplicate products | 0 rows with duplicate `product_id` |
| DQ-S2 | `silver.dim_product_pricing_scd` | SCD Type 2 integrity | every `product_id` has exactly 1 row where `is_current = true` |
| DQ-S3 | `silver.dim_product_pricing_scd` | No negative prices | 0 rows where `list_price < 0` |

### Why DQ-S2 matters
SCD Type 2 requires exactly one "current" price per product at any point in time. If a product has zero current rows, downstream Gold joins will drop that product from enrichment. If it has two or more current rows, joins will produce duplicate order records. This check catches both cases.

---

## Gold Layer Checks

The Gold checks validate that analytics-ready tables are correct and that derived business metrics are reliable.

| Check ID | Table | What is validated | Pass condition |
|---|---|---|---|
| DQ-G1a | `gold.fact_orders` | Table is non-empty | row count > 0 |
| DQ-G1b | `gold.fact_orders` | Primary key integrity | no NULL `order_id` |
| DQ-G2 | `gold.fact_orders` | Valid pricing | 0 rows where `unit_price <= 0` |
| DQ-G3 | `gold.fact_orders` | Late-arrival flagging | every order with `arrival_lag_hours > 48` has `late_arrival_flag = true` |
| DQ-G4 | `gold.fact_user_events` | Primary key integrity | no NULL `event_id` |
| DQ-G5 | `gold.ecommerce_summary` | Date completeness | 0 rows where `summary_date` is NULL |
| DQ-G6 | `gold.ml_session_conversion` | Binary label validity | `converted` column contains only 0 or 1 |

### Why DQ-G3 matters
Orders that arrive more than 48 hours after their `event_time` are late arrivals that need to be flagged for SLA reporting. If late orders exist but are not flagged, the late-arrival analysis in the dashboard will under-report the problem.

### Why DQ-G6 matters
`ml_session_conversion.converted` is used as the target label for a binary classification model. A value outside {0, 1} would corrupt model training.

---

## Check Summary by Layer

| Layer | Checks | Tables covered |
|---|---|---|
| Bronze | 10 (5 tables × row-count + null-PK) | orders, product_catalog, product_pricing, reviews, user_events |
| Silver | 3 | dim_product, dim_product_pricing_scd |
| Gold | 7 | fact_orders, fact_user_events, ecommerce_summary, ml_session_conversion |
| **Total** | **20** | **9 tables** |

---

## Bonus: Great Expectations Validation

In addition to the custom DQ checks, `processing/jobs/great_expectations_validation.py` generates two Great Expectations-compatible artifacts.

### Expectation Suite (`great_expectations_suite.json`)

The suite defines expectations for key columns across all three layers:

| Table | Expectation | Parameters |
|---|---|---|
| `bronze.orders` | `expect_column_values_to_not_be_null` | `order_id` |
| `bronze.orders` | `expect_table_row_count_to_be_between` | min=1 |
| `silver.dim_product` | `expect_column_values_to_be_unique` | `product_id` |
| `silver.dim_product_pricing_scd` | `expect_column_values_to_not_be_null` | `is_current` |
| `gold.fact_orders` | `expect_column_values_to_be_between` | `unit_price` min=0 |
| `gold.ml_session_conversion` | `expect_column_values_to_be_in_set` | `converted` in {0, 1} |

### Validation Result (`great_expectations_validation_result.json`)

Written after each pipeline run. Contains per-expectation pass/fail results, observed values, and run metadata. The job exits with code 1 if any expectation fails, so the Airflow DAG halts before the dashboard is built.

---

## Bonus: DataHub Lineage

`processing/jobs/emit_datahub_lineage.py` writes `processing/jobs/datahub_lineage.json`, which documents:

- Dataset schemas (column names and types) for every Bronze, Silver, and Gold table
- Row counts at time of run
- Job-level lineage edges:

```
CSV Files ──► bronze_ingestion  ──► Bronze tables
Kafka      ──► streaming_consumer ──► bronze.user_events_stream
Bronze     ──► silver_dimensions ──► Silver tables
Silver     ──► gold_facts        ──► Gold tables
```

If `DATAHUB_GMS_URL` is set in the environment, the job also emits Metadata Change Proposals (MCPs) to a live DataHub GMS server.
