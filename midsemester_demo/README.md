# Mid-Semester Demo

This folder is a lightweight demo for the Data Engineering Mid-Semester Project.

It uses:

```text
CSV files -> Python -> SQLite warehouse -> HTML dashboard + quality report
```

It does not use Docker, Kafka, Spark, Airflow, Iceberg, or MinIO. Those are for the final project.

## Run

From PowerShell:

```powershell
cd "C:\Users\10\Desktop\Data Engineer MidS"
python .\midsemester_demo\build_midsemester_demo.py
```

Or:

```powershell
.\midsemester_demo\run_demo.ps1
```

No external Python packages are required.

## Outputs

The script creates:

```text
midsemester_demo\demo_outputs\amazon_midsemester_demo.sqlite
midsemester_demo\demo_outputs\dashboard.html
midsemester_demo\demo_outputs\quality_report.json
```

Open the dashboard in your browser:

```text
midsemester_demo\demo_outputs\dashboard.html
```

## Expected Result

The terminal should end with:

```text
Mid-semester demo built successfully.
Quality: 9/9 passed
```

Expected loaded rows:

```text
bronze_user_activity_events: 75000
bronze_orders_late_arrivals: 30000
bronze_product_catalog_static: 12000
bronze_product_pricing_scd_type2: 41922
bronze_reviews_batch_api: 25000
```

## Tables Created

Bronze:

- `bronze_user_activity_events`
- `bronze_orders_late_arrivals`
- `bronze_product_catalog_static`
- `bronze_product_pricing_scd_type2`
- `bronze_reviews_batch_api`

Silver facts:

- `fact_user_event`
- `fact_order`
- `fact_review`

Silver dimensions:

- `dim_product`
- `dim_category_static`
- `dim_product_pricing_scd`
- `dim_customer_anonymous`
- `dim_channel`
- `dim_date`

Gold:

- `gold_daily_sales_summary`
- `gold_conversion_funnel`
- `gold_review_satisfaction_summary`
- `ml_session_conversion_features`

## What To Present

Show:

1. Business context.
2. Data sources.
3. Bronze, silver, gold architecture.
4. Facts and dimensions.
5. Type 2 SCD pricing dimension.
6. Dashboard HTML.
7. ML feature table.
8. Quality report.
