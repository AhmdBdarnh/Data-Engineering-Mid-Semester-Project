# Data Engineering Mid-Semester Project

This repository contains a mid-semester data warehouse design demo for an Amazon-style ecommerce marketplace.

The demo uses:

```text
CSV files -> Python -> SQLite warehouse -> HTML dashboard + quality report
```

It intentionally does not use Docker, Kafka, Spark, Airflow, Iceberg, or MinIO. Those technologies are for the final project.

## Business Question

Which categories, products, and channels generate the highest revenue and conversion, and where do users drop before purchase?

## Run The Demo

From PowerShell:

```powershell
python .\midsemester_demo\build_midsemester_demo.py
```

Expected result:

```text
Mid-semester demo built successfully.
Quality: 9/9 passed
```

Open the dashboard:

```text
midsemester_demo\demo_outputs\dashboard.html
```

## Main Files

- `midsemester_demo/build_midsemester_demo.py`
- `midsemester_demo/docs/architecture.md`
- `midsemester_demo/docs/data_model.md`
- `midsemester_demo/docs/presentation_outline.md`
- `README_DATA_DICTIONARY.md`

## Data Sources

- `amazon_user_activity_streaming_events.csv`
- `amazon_orders_late_arrivals.csv`
- `amazon_product_catalog_static_dimension.csv`
- `amazon_product_pricing_scd_type2.csv`
- `amazon_reviews_batch_api.csv`
