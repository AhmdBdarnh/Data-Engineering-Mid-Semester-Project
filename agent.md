# Agent Notes

## Folder Purpose

This folder is now dedicated to the **Data Engineering Mid-Semester Project** only.

It contains the raw ecommerce datasets and a lightweight demo that proves the data warehouse design without using final-project technologies like Docker, Kafka, Spark, Airflow, Iceberg, or MinIO.

## Current Folder Contents

```text
Data Engineer MidS/
├── amazon_orders_late_arrivals.csv
├── amazon_product_catalog_static_dimension.csv
├── amazon_product_pricing_scd_type2.csv
├── amazon_reviews_batch_api.csv
├── amazon_user_activity_streaming_events.csv
├── README_DATA_DICTIONARY.md
├── agent.md
└── midsemester_demo/
```


## Mid-Semester Project Goal

Design and present a data warehouse solution for an Amazon-style ecommerce marketplace.

The project should demonstrate:

- At least 3 data sources.
- One streaming-style source.
- One late-arriving source.
- One static dimension.
- One Type 2 Slowly Changing Dimension.
- Bronze, silver, and gold architecture.
- Fact and dimension modeling.
- Business dashboard design.
- ML feature table design.
- Data quality checks.

## Business Case

Business:

```text
Amazon-style ecommerce marketplace
```

Main business question:

```text
Which categories, products, and channels generate the highest revenue and conversion,
and where do users drop before purchase?
```

ML problem:

```text
Predict whether a user session will convert to purchase.
```

## Dataset Mapping

| File | Role |
| --- | --- |
| `amazon_user_activity_streaming_events.csv` | Streaming-style user events source |
| `amazon_orders_late_arrivals.csv` | Late-arriving order source, up to 48 hours |
| `amazon_product_catalog_static_dimension.csv` | Static product/category dimension |
| `amazon_product_pricing_scd_type2.csv` | Type 2 SCD pricing history |
| `amazon_reviews_batch_api.csv` | Additional batch/API review source |

## Demo Folder

The working demo is here:

```text
midsemester_demo/
```

Important files:

```text
midsemester_demo/build_midsemester_demo.py
midsemester_demo/run_demo.ps1
midsemester_demo/README.md
midsemester_demo/agent.md
midsemester_demo/docs/architecture.md
midsemester_demo/docs/data_model.md
midsemester_demo/docs/presentation_outline.md
```

Generated outputs:

```text
midsemester_demo/demo_outputs/amazon_midsemester_demo.sqlite
midsemester_demo/demo_outputs/dashboard.html
midsemester_demo/demo_outputs/quality_report.json
```

## How To Run

From PowerShell:

```powershell
cd "C:\Users\10\Desktop\Data Engineer MidS"
python .\midsemester_demo\build_midsemester_demo.py
```

Or:

```powershell
.\midsemester_demo\run_demo.ps1
```

## Expected Output

Expected terminal result:

```text
Mid-semester demo built successfully.
Quality: 9/9 passed
```

Expected loaded row counts:

```text
bronze_user_activity_events: 75000
bronze_orders_late_arrivals: 30000
bronze_product_catalog_static: 12000
bronze_product_pricing_scd_type2: 41922
bronze_reviews_batch_api: 25000
```

Expected quality summary:

```json
{
  "summary": {
    "checks": 9,
    "passed": 9,
    "failed": 0
  }
}
```

## What To Show In Presentation

1. `midsemester_demo/docs/architecture.md`
2. `midsemester_demo/docs/data_model.md`
3. `midsemester_demo/demo_outputs/dashboard.html`
4. `midsemester_demo/demo_outputs/quality_report.json`

## Scope Boundary

This folder is for mid-semester only.
