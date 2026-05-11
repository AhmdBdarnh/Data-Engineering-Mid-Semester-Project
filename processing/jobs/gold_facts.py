"""
gold_facts.py
──────────────
Builds the four Gold layer tables:

  demo.gold.fact_orders                  ← bronze.orders + silver.dim_product
  demo.gold.fact_user_events             ← bronze.user_events
  demo.gold.ecommerce_summary            ← UNION ALL of orders / events / reviews
  demo.gold.ml_session_conversion        ← session rollup from fact_user_events

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/gold_facts.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round as spark_round, to_date, lit, when,
    count, countDistinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    unix_timestamp,
)


def get_spark():
    return SparkSession.builder.appName("GoldFacts").getOrCreate()


# ── 1. fact_orders ────────────────────────────────────────────────────────────

def build_fact_orders(spark):
    orders = spark.table("demo.bronze.orders")
    products = spark.table("demo.silver.dim_product").select(
        "product_id", "category", "subcategory", "brand", "price_tier"
    )

    df = (
        orders.join(products, on="product_id", how="left")
        .withColumn("order_date", to_date("event_time"))
        .withColumn(
            "arrival_lag_hours",
            spark_round(
                (unix_timestamp("arrival_time") - unix_timestamp("event_time")) / 3600,
                2
            ),
        )
        .select(
            "order_id",
            "order_date",
            "event_time",
            "arrival_time",
            "arrival_lag_hours",
            "customer_id",
            "product_id",
            "category",
            "subcategory",
            "brand",
            "price_tier",
            "quantity",
            "unit_price",
            "discount_amount",
            "shipping_fee",
            "total_amount",
            "payment_status",
            "shipping_status",
            "warehouse_id",
            "delivery_partner",
            "late_arrival_flag",
            "source_system",
        )
    )

    df.writeTo("demo.gold.fact_orders").createOrReplace()
    return df.count()


# ── 2. fact_user_events ───────────────────────────────────────────────────────

def build_fact_user_events(spark):
    df = (
        spark.table("demo.bronze.user_events")
        .withColumn("event_date", to_date("event_time"))
        .withColumn(
            "ingestion_lag_minutes",
            spark_round(
                (unix_timestamp("ingestion_time") - unix_timestamp("event_time")) / 60,
                2
            ),
        )
        .select(
            "event_id",
            "event_date",
            "event_time",
            "ingestion_time",
            "ingestion_lag_minutes",
            "customer_id",
            "session_id",
            "event_type",
            "product_id",
            "device_type",
            "traffic_channel",
            "region",
            "quantity",
            "user_agent_family",
        )
    )

    df.writeTo("demo.gold.fact_user_events").createOrReplace()
    return df.count()


# ── 3. ecommerce_summary ──────────────────────────────────────────────────────

def build_ecommerce_summary(spark):
    """
    One wide table with three row types (distinguished by which metric columns
    are non-null): sales rows, event rows, and review rows.
    """
    fact_orders  = spark.table("demo.gold.fact_orders")
    fact_events  = spark.table("demo.gold.fact_user_events")
    reviews      = spark.table("demo.bronze.reviews")
    dim_product  = spark.table("demo.silver.dim_product").select(
        "product_id", "category", "subcategory", "brand"
    )

    # Sales rows — daily order aggregates
    sales = (
        fact_orders
        .groupBy("order_date", "category", "subcategory", "brand", "shipping_status")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("quantity").alias("units_sold"),
            spark_round(spark_sum("total_amount"), 2).alias("gross_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_round(avg("arrival_lag_hours"), 2).alias("avg_arrival_lag_hours"),
        )
        .select(
            col("order_date").alias("summary_date"),
            "category", "subcategory", "brand",
            col("shipping_status"),
            lit(None).cast("string").alias("traffic_channel"),
            lit(None).cast("string").alias("device_type"),
            lit(None).cast("string").alias("event_type"),
            "orders", "units_sold", "gross_revenue", "avg_order_value",
            "avg_arrival_lag_hours",
            lit(None).cast("long").alias("event_count"),
            lit(None).cast("long").alias("sessions"),
            lit(None).cast("long").alias("reviews"),
            lit(None).cast("double").alias("avg_rating"),
        )
    )

    # Event rows — daily activity aggregates
    events = (
        fact_events
        .groupBy("event_date", "traffic_channel", "device_type", "event_type")
        .agg(
            count("*").alias("event_count"),
            countDistinct("session_id").alias("sessions"),
        )
        .select(
            col("event_date").alias("summary_date"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("string").alias("subcategory"),
            lit(None).cast("string").alias("brand"),
            lit(None).cast("string").alias("shipping_status"),
            "traffic_channel", "device_type", "event_type",
            lit(None).cast("long").alias("orders"),
            lit(None).cast("long").alias("units_sold"),
            lit(None).cast("double").alias("gross_revenue"),
            lit(None).cast("double").alias("avg_order_value"),
            lit(None).cast("double").alias("avg_arrival_lag_hours"),
            "event_count", "sessions",
            lit(None).cast("long").alias("reviews"),
            lit(None).cast("double").alias("avg_rating"),
        )
    )

    # Review rows — daily review aggregates (bronze → gold directly)
    review_rows = (
        reviews
        .join(dim_product, on="product_id", how="left")
        .withColumn("review_date", to_date("review_time"))
        .groupBy("review_date", "category", "subcategory", "brand")
        .agg(
            count("*").alias("reviews"),
            spark_round(avg(col("rating").cast("double")), 3).alias("avg_rating"),
        )
        .select(
            col("review_date").alias("summary_date"),
            "category", "subcategory", "brand",
            lit(None).cast("string").alias("shipping_status"),
            lit(None).cast("string").alias("traffic_channel"),
            lit(None).cast("string").alias("device_type"),
            lit(None).cast("string").alias("event_type"),
            lit(None).cast("long").alias("orders"),
            lit(None).cast("long").alias("units_sold"),
            lit(None).cast("double").alias("gross_revenue"),
            lit(None).cast("double").alias("avg_order_value"),
            lit(None).cast("double").alias("avg_arrival_lag_hours"),
            lit(None).cast("long").alias("event_count"),
            lit(None).cast("long").alias("sessions"),
            "reviews", "avg_rating",
        )
    )

    df = sales.unionAll(events).unionAll(review_rows)
    df.writeTo("demo.gold.ecommerce_summary").createOrReplace()
    return df.count()


# ── 4. ml_session_conversion ──────────────────────────────────────────────────

def build_ml_session_conversion(spark):
    """
    One row per session. Target = converted (1 if any 'purchase' event).
    Features: event counts by type, session duration, device/channel.
    """
    events = spark.table("demo.gold.fact_user_events")

    df = (
        events
        .groupBy("session_id", "customer_id", "device_type", "traffic_channel", "region")
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "page_view",        1).otherwise(0)).alias("page_views"),
            spark_sum(when(col("event_type") == "product_view",     1).otherwise(0)).alias("product_views"),
            spark_sum(when(col("event_type") == "add_to_cart",      1).otherwise(0)).alias("add_to_cart"),
            spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_from_cart"),
            spark_sum(when(col("event_type") == "search",           1).otherwise(0)).alias("searches"),
            spark_sum(when(col("event_type") == "checkout_start",   1).otherwise(0)).alias("checkout_starts"),
            spark_sum(when(col("event_type") == "purchase_click",   1).otherwise(0)).alias("purchase_clicks"),
            spark_round(
                (unix_timestamp(spark_max("event_time")) - unix_timestamp(spark_min("event_time"))) / 60,
                2
            ).alias("session_duration_minutes"),
            countDistinct("product_id").alias("distinct_products_viewed"),
            spark_min("event_date").alias("session_date"),
            # converted = 1 if the session reached purchase_click
            when(
                spark_sum(when(col("event_type") == "purchase_click", 1).otherwise(0)) > 0, 1
            ).otherwise(0).alias("converted"),
        )
    )

    df.writeTo("demo.gold.ml_session_conversion").createOrReplace()
    return df.count()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Gold Layer — Facts & Aggregates")
    print(f"{sep}\n")

    count1 = build_fact_orders(spark)
    print(f"  ✓ demo.gold.fact_orders            {count1:>7,} rows")

    count2 = build_fact_user_events(spark)
    print(f"  ✓ demo.gold.fact_user_events       {count2:>7,} rows")

    count3 = build_ecommerce_summary(spark)
    print(f"  ✓ demo.gold.ecommerce_summary      {count3:>7,} rows")

    count4 = build_ml_session_conversion(spark)
    print(f"  ✓ demo.gold.ml_session_conversion  {count4:>7,} rows")

    print(f"\n  Sample conversion rate:")
    spark.sql("""
        SELECT
            converted,
            COUNT(*)                                    AS sessions,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM demo.gold.ml_session_conversion
        GROUP BY converted
        ORDER BY converted
    """).show()

    print(f"\n{sep}")
    print("  ✓ Gold layer built successfully!")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
