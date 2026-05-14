import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sep = "=" * 50
print(f"\n{sep}")
failed = False

try:
    n = spark.table("demo.bronze.user_events_stream").count()
    print(f"  demo.bronze.user_events_stream : {n:,} rows")
    if n == 0:
        print("  WARNING: streaming table is empty")
        failed = True
except Exception as e:
    print(f"  ERROR reading user_events_stream: {e}")
    failed = True

try:
    n2 = spark.table("demo.bronze.user_events").count()
    print(f"  demo.bronze.user_events (batch) : {n2:,} rows")
except Exception as e:
    print(f"  ERROR reading user_events: {e}")

print(f"{sep}\n")
spark.stop()

if failed:
    sys.exit(1)
