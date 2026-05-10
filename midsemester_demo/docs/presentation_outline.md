# Presentation Outline

## 1. Business Context

Amazon-style ecommerce marketplace.

Main question:

```text
Which categories, products, and channels generate the highest revenue and conversion?
```

## 2. Data Sources

- Streaming user events.
- Late-arriving orders.
- Static product catalog.
- Product pricing SCD Type 2.
- Batch/API reviews.

## 3. Architecture

Show bronze, silver, and gold layers.

## 4. Data Model

Show:

- `fact_user_event`
- `fact_order`
- `fact_review`
- `dim_product`
- `dim_category_static`
- `dim_product_pricing_scd`
- `dim_customer_anonymous`
- `dim_channel`
- `dim_date`

## 5. Dashboard

Open:

```text
midsemester_demo\demo_outputs\dashboard.html
```

## 6. ML Use Case

Predict session conversion using:

```text
ml_session_conversion_features
```

## 7. Data Quality

Open:

```text
midsemester_demo\demo_outputs\quality_report.json
```
