# Amazon-Style eCommerce Synthetic Datasets

These datasets are synthetic and designed for the Data Engineering Mid-Semester Project.

Business: Amazon-style eCommerce Marketplace.

Files:
1. amazon_user_activity_streaming_events.csv
   - Rows: 75000
   - Purpose: Streaming / real-time source.
   - Grain: One user event.
   - Main keys: event_id, customer_id, product_id, session_id.
   - Use: Bronze streaming ingestion, conversion funnel, abandoned cart analysis.

2. amazon_orders_late_arrivals.csv
   - Rows: 30000
   - Purpose: Late-arriving source.
   - Grain: One order line/event.
   - Main keys: order_id, customer_id, product_id.
   - Special fields: event_time, arrival_time, late_arrival_flag.
   - Late arrival rule: some records arrive up to 48 hours after event_time.
   - Use: order facts, revenue analytics, out-of-order processing.

3. amazon_product_catalog_static_dimension.csv
   - Rows: 12000
   - Purpose: Static dimension source.
   - Grain: One product.
   - Main key: product_id.
   - Recommended dimension: dim_product and dim_category.
   - Static dimension example: dim_category.

4. amazon_product_pricing_scd_type2.csv
   - Rows: 41922
   - Purpose: Slowly Changing Dimension Type 2 source.
   - Grain: One historical price version per product.
   - Main keys: pricing_sk, product_id.
   - SCD fields: effective_from, effective_to, is_current.
   - Recommended SCD table: dim_product_pricing_scd.

5. amazon_reviews_batch_api.csv
   - Rows: 25000
   - Purpose: Additional batch/API source.
   - Grain: One product review.
   - Main keys: review_id, customer_id, product_id.
   - Use: customer satisfaction dashboard and ML feature table.

Recommended business question:
Which categories, products, and channels generate the highest revenue and conversion, and where do we lose users before purchase?

Recommended dashboard:
Revenue by category, conversion funnel, top products, delayed shipments, return/refund rate, average rating by category.

Recommended ML problem:
Predict whether a user session will convert to a purchase or predict risk of cart abandonment.

Suggested DW tables:
- fact_user_event
- fact_order
- fact_review
- dim_product
- dim_category_static
- dim_customer_anonymous
- dim_product_pricing_scd
- dim_date
- dim_channel
- gold_daily_sales_summary
- gold_conversion_funnel
- ml_session_conversion_features
