"""
silver_dimensions.py
─────────────────────
Builds the two Silver dimension tables from Bronze:

  demo.silver.dim_product             ← bronze.product_catalog
  demo.silver.dim_product_pricing_scd ← bronze.product_pricing  (SCD Type 2)

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/silver_dimensions.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, current_date, when, count, lit, round as spark_round
)


def get_spark():
    return SparkSession.builder.appName("SilverDimensions").getOrCreate()


def build_dim_product(spark):
    """
    Clean product catalog dimension.
    Adds days_on_market derived column. Drops rows missing the primary key.
    """
    df = (
        spark.table("demo.bronze.product_catalog")
        .filter(col("product_id").isNotNull())
        .filter(col("product_name").isNotNull())
        .withColumn(
            "days_on_market",
            datediff(current_date(), col("launch_date"))
        )
        .withColumn(
            "price_tier",
            when(col("base_price") < 25,  lit("budget"))
            .when(col("base_price") < 100, lit("mid"))
            .when(col("base_price") < 300, lit("premium"))
            .otherwise(lit("luxury"))
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "seller_id",
            "launch_date",
            "base_price",
            "price_tier",
            "days_on_market",
            "warehouse_id",
            "is_active",
            "weight_kg",
            "product_rating_initial",
        )
    )

    df.writeTo("demo.silver.dim_product").createOrReplace()
    return df.count()


def build_dim_product_pricing_scd(spark):
    """
    SCD Type 2 pricing dimension.
    Source is already SCD-structured (pricing_sk, effective_from, effective_to,
    is_current). Silver layer validates and propagates it intact.
    Adds discount_amount derived column.
    """
    df = (
        spark.table("demo.bronze.product_pricing")
        .filter(col("pricing_sk").isNotNull())
        .filter(col("product_id").isNotNull())
        .withColumn(
            "discount_amount",
            spark_round(col("list_price") - col("final_price"), 2)
        )
        .select(
            "pricing_sk",
            "product_id",
            "seller_id",
            "category",
            "list_price",
            "discount_pct",
            "discount_amount",
            "final_price",
            "currency",
            "effective_from",
            "effective_to",
            "is_current",
            "change_reason",
        )
    )

    df.writeTo("demo.silver.dim_product_pricing_scd").createOrReplace()
    return df.count()


def validate(spark):
    """Quick sanity checks printed after loading."""
    print("\n  Validation:")

    # Every product should have exactly one is_current = true price row
    current_counts = (
        spark.table("demo.silver.dim_product_pricing_scd")
        .filter(col("is_current") == True)
        .groupBy("product_id")
        .agg(count("*").alias("n"))
        .filter(col("n") != 1)
        .count()
    )
    if current_counts == 0:
        print("  ✓ SCD integrity: every product has exactly one current price row")
    else:
        print(f"  ✗ SCD integrity: {current_counts} products have != 1 current price row")

    # All dim_product products should have at least one pricing row
    orphaned = (
        spark.table("demo.silver.dim_product")
        .join(
            spark.table("demo.silver.dim_product_pricing_scd")
            .select("product_id").distinct(),
            on="product_id",
            how="left_anti"
        )
        .count()
    )
    if orphaned == 0:
        print("  ✓ Referential integrity: all products have at least one pricing row")
    else:
        print(f"  ✗ Referential integrity: {orphaned} products have no pricing rows")

    # Price tier distribution
    print("\n  Price tier distribution:")
    (
        spark.table("demo.silver.dim_product")
        .groupBy("price_tier")
        .count()
        .orderBy("price_tier")
        .show()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Silver Layer — Dimension Tables")
    print(f"{sep}\n")

    count1 = build_dim_product(spark)
    print(f"  ✓ demo.silver.dim_product             {count1:>7,} rows")

    count2 = build_dim_product_pricing_scd(spark)
    print(f"  ✓ demo.silver.dim_product_pricing_scd {count2:>7,} rows")

    validate(spark)

    print(f"\n{sep}")
    print("  ✓ Silver dimensions built successfully!")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
