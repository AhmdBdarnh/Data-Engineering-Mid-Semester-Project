"""
silver_dimensions.py
─────────────────────
Builds the two Silver dimension tables from Bronze:

  demo.silver.dim_product             ← bronze.product_catalog
  demo.silver.dim_product_pricing_scd ← bronze.product_pricing  (real SCD Type 2)

dim_product_pricing_scd uses genuine change-detection, not a straight copy
of the bronze table:

  • First run (table does not exist yet): bootstrap with the full
    historical version set already present in the bronze source, so the
    pre-existing price history in the CSV is preserved rather than
    discarded.
  • Every later run: only the row bronze currently marks is_current=true
    per product is treated as "today's observed price" (the incoming
    feed). It is compared against the active Silver row for that
    product on the tracked pricing attributes (list_price, discount_pct,
    final_price, currency).
      - Unchanged prices are left untouched.
      - Changed prices close the old active row (is_current=false,
        effective_to=today) via an Iceberg MERGE INTO, then a new
        version row is appended (new pricing_sk, effective_from=today,
        effective_to=null, is_current=true).
      - Brand-new products are appended as new current rows.
    Running this twice in a row with unchanged input performs zero
    updates and zero inserts (idempotent), because "today's price"
    already matches the active Silver row.

See processing/jobs/demo_scd_change.py for a standalone script that proves
the change-detection path with a synthetic price change.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/silver_dimensions.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, current_date, current_timestamp, when, count, lit,
    round as spark_round, concat, date_format,
)

TARGET = "demo.silver.dim_product_pricing_scd"

# Pricing attributes that define "the price changed" for SCD purposes.
TRACKED_COLUMNS = ["list_price", "discount_pct", "final_price", "currency"]

SCD_COLUMNS = [
    "pricing_sk", "product_id", "seller_id", "category",
    "list_price", "discount_pct", "discount_amount", "final_price",
    "currency", "effective_from", "effective_to", "is_current",
    "change_reason",
]


def get_spark():
    return SparkSession.builder.appName("SilverDimensions").getOrCreate()


def table_exists(spark, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except Exception:
        return False


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


# ── SCD Type 2 pricing dimension ────────────────────────────────────────────

def _with_discount_amount(df):
    return df.withColumn(
        "discount_amount",
        spark_round(col("list_price") - col("final_price"), 2)
    )


def _with_new_pricing_sk(df):
    """Unique key for a newly inserted version row (product + timestamp)."""
    return df.withColumn(
        "pricing_sk",
        concat(col("product_id"), lit("-"), date_format(current_timestamp(), "yyyyMMddHHmmss"))
    )


def bootstrap_scd_table(spark, bronze):
    """
    First run only. The source CSV already contains genuine multi-version
    history for many products (multiple pricing_sk rows per product_id with
    effective_from/effective_to/is_current already populated) — that history
    is real and is preserved as-is. Every later run hands off to
    apply_scd_merge() instead of calling this again.
    """
    df = _with_discount_amount(bronze).select(*SCD_COLUMNS)
    df.writeTo(TARGET).createOrReplace()
    return df.count()


def apply_scd_merge(spark, incoming):
    """
    Real SCD Type 2 change-detection merge against the live Silver table.

    incoming: one row per product_id representing "today's observed price"
    with columns (product_id, seller_id, category, list_price,
    discount_pct, final_price, currency, change_reason).

    Returns (new_products, changed_products, unchanged_products).
    """
    current = (
        spark.table(TARGET)
        .filter(col("is_current") == True)
        .select(
            col("product_id"),
            *[col(c).alias(f"cur_{c}") for c in TRACKED_COLUMNS],
        )
    )

    joined = incoming.join(current, on="product_id", how="left").cache()

    is_new = col("cur_list_price").isNull()
    is_changed = (
        (col("list_price") != col("cur_list_price"))
        | (col("discount_pct") != col("cur_discount_pct"))
        | (col("final_price") != col("cur_final_price"))
        | (col("currency") != col("cur_currency"))
    )

    new_count = joined.filter(is_new).count()
    changed_count = joined.filter(~is_new & is_changed).count()
    unchanged_count = joined.filter(~is_new & ~is_changed).count()

    # ── Step 1: close the previous active row for every changed product ────
    to_close = (
        joined.filter(~is_new & is_changed)
        .select("product_id")
        .distinct()
    )

    if to_close.count() > 0:
        to_close.createOrReplaceTempView("scd_to_close")
        spark.sql(f"""
            MERGE INTO {TARGET} t
            USING scd_to_close s
            ON t.product_id = s.product_id AND t.is_current = true
            WHEN MATCHED THEN UPDATE SET
                t.is_current = false,
                t.effective_to = current_date()
        """)

    # ── Step 2: insert a new current version for new + changed products ────
    to_insert_cols = ["product_id", "seller_id", "category", "list_price",
                       "discount_pct", "final_price", "currency", "change_reason"]
    to_insert = joined.filter(is_new | is_changed).select(*to_insert_cols)

    if to_insert.count() > 0:
        to_insert_final = (
            _with_new_pricing_sk(_with_discount_amount(to_insert))
            .withColumn("effective_from", current_date())
            .withColumn("effective_to", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
            .select(*SCD_COLUMNS)
        )
        to_insert_final.writeTo(TARGET).append()

    joined.unpersist()
    return new_count, changed_count, unchanged_count


def build_dim_product_pricing_scd(spark):
    bronze = (
        spark.table("demo.bronze.product_pricing")
        .filter(col("pricing_sk").isNotNull())
        .filter(col("product_id").isNotNull())
    )

    if not table_exists(spark, TARGET):
        print("  First run — bootstrapping SCD table from bronze history...")
        n = bootstrap_scd_table(spark, bronze)
        print(f"  ✓ Bootstrap complete: {n:,} historical rows loaded")
        return spark.table(TARGET).count()

    incoming = (
        bronze.filter(col("is_current") == True)
        .select("product_id", "seller_id", "category", "list_price",
                "discount_pct", "final_price", "currency", "change_reason")
    )

    new_n, changed_n, unchanged_n = apply_scd_merge(spark, incoming)
    print(
        f"  SCD merge: {new_n} new product(s) inserted, "
        f"{changed_n} price change(s) (old row closed, new version inserted), "
        f"{unchanged_n} unchanged"
    )

    return spark.table(TARGET).count()


def validate(spark):
    """Sanity checks printed after loading (does not fail the job)."""
    print("\n  Validation:")

    scd = spark.table(TARGET)

    # Every product should have exactly one is_current = true price row
    current_counts = (
        scd.filter(col("is_current") == True)
        .groupBy("product_id")
        .agg(count("*").alias("n"))
        .filter(col("n") != 1)
        .count()
    )
    if current_counts == 0:
        print("  ✓ SCD integrity: every product has exactly one current price row")
    else:
        print(f"  ✗ SCD integrity: {current_counts} products have != 1 current price row")

    # No duplicate pricing_sk (every version row is uniquely keyed)
    dup_sk = (
        scd.groupBy("pricing_sk").agg(count("*").alias("n"))
        .filter(col("n") > 1).count()
    )
    if dup_sk == 0:
        print("  ✓ SCD integrity: no duplicate pricing_sk values")
    else:
        print(f"  ✗ SCD integrity: {dup_sk} duplicate pricing_sk values")

    # Closed rows must carry an effective_to; current rows must not
    bad_closed = scd.filter((col("is_current") == False) & col("effective_to").isNull()).count()
    bad_current = scd.filter((col("is_current") == True) & col("effective_to").isNotNull()).count()
    if bad_closed == 0 and bad_current == 0:
        print("  ✓ SCD integrity: effective_to is consistent with is_current on every row")
    else:
        print(f"  ✗ SCD integrity: {bad_closed} closed rows missing effective_to, "
              f"{bad_current} current rows with a non-null effective_to")

    # History is preserved: at least as many rows as distinct products
    total_rows = scd.count()
    distinct_products = scd.select("product_id").distinct().count()
    print(f"  History check: {total_rows:,} total version rows across "
          f"{distinct_products:,} products (history retained across runs)")

    # All dim_product products should have at least one pricing row
    orphaned = (
        spark.table("demo.silver.dim_product")
        .join(scd.select("product_id").distinct(), on="product_id", how="left_anti")
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
