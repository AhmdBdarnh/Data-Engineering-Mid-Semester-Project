from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sep = "=" * 50
print(f"\n{sep}")
try:
    n = spark.table("demo.bronze.user_events_stream").count()
    print(f"  demo.bronze.user_events_stream : {n:,} rows")
except Exception as e:
    print(f"  ERROR: {e}")

try:
    n2 = spark.table("demo.bronze.user_events").count()
    print(f"  demo.bronze.user_events (batch) : {n2:,} rows")
except Exception as e:
    print(f"  ERROR: {e}")

print(f"{sep}\n")
spark.stop()
