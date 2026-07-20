"""
test_connection.py
──────────────────
Verifies that Spark, the Iceberg REST catalog, and MinIO are all wired
together correctly.

Run with:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/bitnami/spark/jobs/test_connection.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType


def main():
    spark = (
        SparkSession.builder
        .appName("IcebergConnectionTest")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Iceberg + MinIO + Spark connection test")
    print(f"{sep}\n")

    # ── Create namespaces for the three lakehouse layers ──────────────────────
    for namespace in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS demo.{namespace}")
        print(f"  ✓ Namespace  demo.{namespace}  ready")

    # ── Create a simple Iceberg table in bronze ───────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.bronze.connection_test (
            id     BIGINT,
            layer  STRING,
            value  DOUBLE
        )
        USING iceberg
    """)
    print("\n  ✓ Iceberg table  demo.bronze.connection_test  created")

    # ── Write three test rows ─────────────────────────────────────────────────
    schema = StructType([
        StructField("id",    LongType(),   nullable=False),
        StructField("layer", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
    ])
    rows = [(1, "bronze", 1.0), (2, "silver", 2.0), (3, "gold", 3.0)]
    df   = spark.createDataFrame(rows, schema)
    df.writeTo("demo.bronze.connection_test").append()
    print("  ✓ Test rows written to MinIO via Iceberg")

    # ── Read back and display ─────────────────────────────────────────────────
    result = spark.sql("SELECT * FROM demo.bronze.connection_test ORDER BY id")
    print("\n  Table contents:")
    result.show()

    count = result.count()
    print(f"\n{sep}")
    if count >= 3:
        print("  ✓ SUCCESS — Spark  ↔  Iceberg REST catalog  ↔  MinIO all working!")
    else:
        print("  ✗ FAIL — row count unexpected, check logs above")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
