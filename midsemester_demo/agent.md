# Mid-Semester Demo Agent Notes

## Purpose

This demo supports the mid-semester project. The mid-semester assignment wants a data warehouse **design and presentation**, not the full final-project implementation.

The demo proves the design locally using the CSV files.

## Business Case

Business:

```text
Amazon-style ecommerce marketplace
```

Business question:

```text
Which categories, products, and channels generate the highest revenue and conversion,
and where do users drop before purchase?
```

ML problem:

```text
Predict whether a user session will convert to purchase.
```

## Input Files

The demo reads these files from the project root:

```text
amazon_user_activity_streaming_events.csv
amazon_orders_late_arrivals.csv
amazon_product_catalog_static_dimension.csv
amazon_product_pricing_scd_type2.csv
amazon_reviews_batch_api.csv
```

Dataset mapping:

| File | Requirement |
| --- | --- |
| `amazon_user_activity_streaming_events.csv` | Streaming source |
| `amazon_orders_late_arrivals.csv` | Late-arriving source |
| `amazon_product_catalog_static_dimension.csv` | Static dimension |
| `amazon_product_pricing_scd_type2.csv` | Type 2 SCD |
| `amazon_reviews_batch_api.csv` | Additional batch/API source |

## Files In This Folder

```text
midsemester_demo/
├── build_midsemester_demo.py
├── run_demo.ps1
├── README.md
├── agent.md
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   └── presentation_outline.md
└── demo_outputs/
    ├── amazon_midsemester_demo.sqlite
    ├── dashboard.html
    └── quality_report.json
```

## How To Run

```powershell
cd "C:\Users\10\Desktop\Data Engineer MidS"
python .\midsemester_demo\build_midsemester_demo.py
```

## Expected Outputs

```text
midsemester_demo\demo_outputs\amazon_midsemester_demo.sqlite
midsemester_demo\demo_outputs\dashboard.html
midsemester_demo\demo_outputs\quality_report.json
```

Expected quality result:

```json
{
  "summary": {
    "checks": 9,
    "passed": 9,
    "failed": 0
  }
}
```

## Scope

This demo is enough for mid-semester because it shows:

- Data source understanding.
- Bronze, silver, gold architecture.
- Fact and dimension modeling.
- Static dimension.
- Type 2 SCD.
- Late-arriving data handling.
- Business dashboard output.
- ML feature table.
- Data quality checks.

The final project should later use Docker, Kafka, Spark, Airflow, Iceberg, and MinIO.
