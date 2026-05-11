"""
bronze_ingestion.py
────────────────────
Reads the 5 raw CSV files from /project_data and writes them as Bronze
Iceberg tables in the demo.bronze namespace.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/bronze_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

DATA_DIR = "/project_data"


def get_spark():
    return SparkSession.builder.appName("BronzeIngestion").getOrCreate()


def ingest_orders(spark):
    df = (
        spark.read.option("header", "true")
        .csv(f"{DATA_DIR}/amazon_orders_late_arrivals.csv")
        .select(
            col("order_id"),
            to_timestamp("event_time").alias("event_time"),
            to_timestamp("arrival_time").alias("arrival_time"),
            col("customer_id"),
            col("product_id"),
            col("quantity").cast("int"),
            col("unit_price").cast("double"),
            col("discount_amount").cast("double"),
            col("shipping_fee").cast("double"),
            col("payment_status"),
            col("shipping_status"),
            col("warehouse_id"),
            col("delivery_partner"),
            col("late_arrival_flag").cast("boolean"),
            col("source_system"),
            col("total_amount").cast("double"),
        )
    )
    df.writeTo("demo.bronze.orders").createOrReplace()
    return df.count()


def ingest_product_catalog(spark):
    df = (
        spark.read.option("header", "true")
        .csv(f"{DATA_DIR}/amazon_product_catalog_static_dimension.csv")
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("subcategory"),
            col("brand"),
            col("seller_id"),
            to_date("launch_date").alias("launch_date"),
            col("base_price").cast("double"),
            col("warehouse_id"),
            col("is_active").cast("boolean"),
            col("weight_kg").cast("double"),
            col("product_rating_initial").cast("double"),
        )
    )
    df.writeTo("demo.bronze.product_catalog").createOrReplace()
    return df.count()


def ingest_product_pricing(spark):
    df = (
        spark.read.option("header", "true")
        .csv(f"{DATA_DIR}/amazon_product_pricing_scd_type2.csv")
        .select(
            col("pricing_sk"),
            col("product_id"),
            col("seller_id"),
            col("category"),
            col("list_price").cast("double"),
            col("discount_pct").cast("double"),
            col("final_price").cast("double"),
            col("currency"),
            to_date("effective_from").alias("effective_from"),
            to_date("effective_to").alias("effective_to"),
            col("is_current").cast("boolean"),
            col("change_reason"),
        )
    )
    df.writeTo("demo.bronze.product_pricing").createOrReplace()
    return df.count()


def ingest_reviews(spark):
    df = (
        spark.read.option("header", "true")
        .csv(f"{DATA_DIR}/amazon_reviews_batch_api.csv")
        .select(
            col("review_id"),
            to_timestamp("review_time").alias("review_time"),
            col("customer_id"),
            col("product_id"),
            col("rating").cast("int"),
            col("review_title"),
            col("verified_purchase").cast("boolean"),
            col("helpful_votes").cast("int"),
            col("sentiment_score").cast("double"),
            to_date("source_file_date").alias("source_file_date"),
            to_timestamp("batch_loaded_at").alias("batch_loaded_at"),
        )
    )
    df.writeTo("demo.bronze.reviews").createOrReplace()
    return df.count()


def ingest_user_events(spark):
    df = (
        spark.read.option("header", "true")
        .csv(f"{DATA_DIR}/amazon_user_activity_streaming_events.csv")
        .select(
            col("event_id"),
            to_timestamp("event_time").alias("event_time"),
            col("customer_id"),
            col("session_id"),
            col("event_type"),
            col("product_id"),
            col("device_type"),
            col("traffic_channel"),
            col("region"),
            col("quantity").cast("int"),
            col("user_agent_family"),
            to_timestamp("ingestion_time").alias("ingestion_time"),
        )
    )
    df.writeTo("demo.bronze.user_events").createOrReplace()
    return df.count()


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Bronze Layer Ingestion")
    print(f"{sep}\n")

    tasks = [
        ("orders",          ingest_orders),
        ("product_catalog", ingest_product_catalog),
        ("product_pricing", ingest_product_pricing),
        ("reviews",         ingest_reviews),
        ("user_events",     ingest_user_events),
    ]

    for name, fn in tasks:
        count = fn(spark)
        print(f"  ✓ demo.bronze.{name:<20} {count:>7,} rows")

    print(f"\n{sep}")
    print("  ✓ All Bronze tables loaded successfully!")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
