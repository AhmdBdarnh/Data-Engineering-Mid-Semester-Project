"""
streaming_consumer.py
──────────────────────
Spark Structured Streaming job that reads JSON events from the Kafka
'user_events' topic and appends them into demo.bronze.user_events_stream
(Iceberg).

Late-arrival handling
─────────────────────
A 48-hour watermark on event_time lets Spark accept events that arrive up
to 48 hours after their event timestamp before dropping their state.  Every
row where the ingestion lag exceeds 60 minutes is also flagged with
is_late_arrival=true so downstream jobs can audit or exclude stale events.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/streaming_consumer.py

The job runs until manually stopped (Ctrl-C / docker stop).
Checkpoints are stored in s3://warehouse/checkpoints/user_events_stream/
so the job can resume from where it left off after a restart.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, to_timestamp, round as spark_round,
    unix_timestamp, current_timestamp, when, lit,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType,
)

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC     = "user_events"
CHECKPOINT_DIR  = "s3a://warehouse/checkpoints/user_events_stream/"

# Schema of each JSON message produced by producer.py
EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType()),
    StructField("event_time",       StringType()),   # parsed to timestamp below
    StructField("customer_id",      StringType()),
    StructField("session_id",       StringType()),
    StructField("event_type",       StringType()),
    StructField("product_id",       StringType()),
    StructField("device_type",      StringType()),
    StructField("traffic_channel",  StringType()),
    StructField("region",           StringType()),
    StructField("quantity",         StringType()),   # cast to int below
    StructField("user_agent_family",StringType()),
    StructField("ingestion_time",   StringType()),   # parsed to timestamp below
    StructField("produced_at",      StringType()),   # added by producer
])


def get_spark():
    return SparkSession.builder.appName("UserEventsStreamConsumer").getOrCreate()


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Spark Structured Streaming — user_events → bronze")
    print(f"{sep}\n")
    print(f"  Source : Kafka {KAFKA_BOOTSTRAP}  topic={KAFKA_TOPIC}")
    print(f"  Sink   : demo.bronze.user_events  (Iceberg append)")
    print(f"  Ckpt   : {CHECKPOINT_DIR}\n")

    # ── Ensure target Iceberg table exists ───────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.bronze.user_events_stream (
            event_id               STRING,
            event_date             DATE,
            event_time             TIMESTAMP,
            ingestion_time         TIMESTAMP,
            ingestion_lag_minutes  DOUBLE,
            is_late_arrival        BOOLEAN,
            stream_loaded_at       TIMESTAMP,
            customer_id            STRING,
            session_id             STRING,
            event_type             STRING,
            product_id             STRING,
            device_type            STRING,
            traffic_channel        STRING,
            region                 STRING,
            quantity               INT,
            user_agent_family      STRING
        ) USING iceberg
    """)
    print("  ✓ Target table demo.bronze.user_events_stream ready\n")

    # ── Read stream from Kafka ────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # replay all retained messages
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────────
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time",    to_timestamp("event_time"))
        .withColumn("ingestion_time", to_timestamp("ingestion_time"))
        .withColumn("event_date",    to_date("event_time"))
        .withColumn("quantity",      col("quantity").cast(IntegerType()))
        .withColumn(
            "ingestion_lag_minutes",
            spark_round(
                (unix_timestamp("ingestion_time") - unix_timestamp("event_time")) / 60,
                2
            )
        )
        # Flag events where ingestion is more than 60 minutes after event time
        .withColumn(
            "is_late_arrival",
            when(col("ingestion_lag_minutes") > 60, lit(True)).otherwise(lit(False))
        )
        .withColumn("stream_loaded_at", current_timestamp())
        .select(
            "event_id",
            "event_date",
            "event_time",
            "ingestion_time",
            "ingestion_lag_minutes",
            "is_late_arrival",
            "stream_loaded_at",
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

    # ── Apply 48-hour watermark for late-arrival tolerance ───────────────────
    # Spark will accept events up to 48 hours past their event_time before
    # discarding state.  This satisfies the late-arriving data requirement.
    watermarked = parsed.withWatermark("event_time", "48 hours")

    # ── Write stream to Iceberg (append mode) ─────────────────────────────────
    query = (
        watermarked.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")   # micro-batch every 30 s
        .option("path", "demo.bronze.user_events_stream")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print("  Streaming query running — micro-batch every 30 s\n")
    print("  (Stop with Ctrl-C)\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
