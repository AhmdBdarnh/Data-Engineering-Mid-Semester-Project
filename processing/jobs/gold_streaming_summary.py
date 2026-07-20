"""
gold_streaming_summary.py
───────────────────────────
Closes the Kafka → Bronze streaming path so it actually reaches the
business layer, instead of dead-ending at demo.bronze.user_events_stream.

  demo.bronze.user_events_stream        (written by streaming_consumer.py)
      │  dedup by event_id, drop rows with a null event_id
      ▼
  demo.silver.user_events_stream_clean  (createOrReplace — a full,
                                          deduplicated snapshot of every
                                          event the streaming consumer has
                                          landed so far)
      │  aggregate by date / event_type / channel / device
      ▼
  demo.gold.realtime_event_summary      (createOrReplace — business-facing
                                          real-time activity summary,
                                          surfaced on the dashboard)

Kept deliberately separate from demo.gold.fact_user_events /
demo.gold.ecommerce_summary: the Kafka producer replays the SAME
amazon_user_activity_streaming_events.csv that bronze_ingestion.py already
loads as demo.bronze.user_events, so merging the two paths into one fact
table would double-count every event. realtime_event_summary reports on
the streaming path only.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/gold_streaming_summary.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, sum as spark_sum, round as spark_round

BRONZE_STREAM = "demo.bronze.user_events_stream"
SILVER_CLEAN = "demo.silver.user_events_stream_clean"
GOLD_SUMMARY = "demo.gold.realtime_event_summary"


def get_spark():
    return SparkSession.builder.appName("GoldStreamingSummary").getOrCreate()


def table_exists(spark, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except Exception:
        return False


def build_silver_clean(spark):
    df = (
        spark.table(BRONZE_STREAM)
        .filter(col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
    )
    df.writeTo(SILVER_CLEAN).createOrReplace()
    return df.count()


def build_gold_summary(spark):
    clean = spark.table(SILVER_CLEAN)

    df = (
        clean
        .groupBy("event_date", "event_type", "traffic_channel", "device_type")
        .agg(
            count("*").alias("event_count"),
            countDistinct("session_id").alias("sessions"),
            spark_sum(col("is_late_arrival").cast("int")).alias("late_arrival_count"),
            spark_round(avg("ingestion_lag_minutes"), 2).alias("avg_ingestion_lag_minutes"),
        )
    )
    df.writeTo(GOLD_SUMMARY).createOrReplace()
    return df.count()


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Gold Streaming Summary — Kafka -> Bronze -> Silver -> Gold")
    print(f"{sep}\n")

    if not table_exists(spark, BRONZE_STREAM):
        print(f"  ✗ {BRONZE_STREAM} does not exist yet — run streaming_consumer.py first")
        spark.stop()
        return

    bronze_n = spark.table(BRONZE_STREAM).count()
    print(f"  {BRONZE_STREAM:<38} {bronze_n:>7,} rows")

    if bronze_n == 0:
        print("  No streaming rows landed yet — nothing to summarize this run.")
        spark.stop()
        return

    clean_n = build_silver_clean(spark)
    print(f"  ✓ {SILVER_CLEAN:<36} {clean_n:>7,} rows")

    dup_check = (
        spark.table(SILVER_CLEAN).groupBy("event_id").agg(count("*").alias("n"))
        .filter(col("n") > 1).count()
    )
    print(f"  {'✓' if dup_check == 0 else '✗'} duplicate event_id in silver clean = {dup_check}")

    gold_n = build_gold_summary(spark)
    print(f"  ✓ {GOLD_SUMMARY:<36} {gold_n:>7,} rows")

    print(f"\n{sep}")
    print("  ✓ Gold streaming summary built successfully!")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
