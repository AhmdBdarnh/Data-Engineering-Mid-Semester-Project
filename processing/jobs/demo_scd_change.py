"""
demo_scd_change.py
────────────────────
Stand-alone proof script for the SCD Type 2 change-detection logic in
silver_dimensions.py. NOT part of any Airflow DAG and not run by
start.sh / start.ps1 — the source CSV (amazon_product_pricing_scd_type2.csv)
is a static file and never changes between pipeline runs, so this script is
the reproducible way to demonstrate that a price change is actually
detected, closes the old row, and inserts a new version.

It does not modify the bronze source or the CSV. It builds one synthetic
"incoming price update" row in memory for a real product_id already present
in demo.silver.dim_product_pricing_scd, and runs it through the exact same
apply_scd_merge() function that the production Silver job uses.

Run with (after silver_dimensions.py has been run at least once):
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/demo_scd_change.py [product_id]

If no product_id is given, the first current product is picked
automatically.
"""

import sys

from pyspark.sql.functions import col

from silver_dimensions import get_spark, apply_scd_merge, TARGET


def show_product(spark, product_id, label):
    print(f"\n  {label} — demo.silver.dim_product_pricing_scd rows for {product_id}:")
    (
        spark.table(TARGET)
        .filter(col("product_id") == product_id)
        .select("pricing_sk", "product_id", "list_price", "final_price",
                "effective_from", "effective_to", "is_current")
        .orderBy("effective_from")
        .show(truncate=False)
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  SCD Type 2 — synthetic change-detection proof")
    print(f"{sep}")

    current = spark.table(TARGET).filter(col("is_current") == True)

    if len(sys.argv) > 1:
        product_id = sys.argv[1]
    else:
        product_id = current.orderBy("product_id").limit(1).collect()[0]["product_id"]

    before_row = current.filter(col("product_id") == product_id).collect()
    if not before_row:
        print(f"  ✗ product_id={product_id} has no current row — pick a different one")
        spark.stop()
        sys.exit(1)
    before_row = before_row[0]

    show_product(spark, product_id, "BEFORE")

    bumped_list_price = float(before_row["list_price"]) + 5.00
    bumped_final_price = float(before_row["final_price"]) + 5.00

    incoming = spark.createDataFrame(
        [(
            product_id,
            before_row["seller_id"],
            before_row["category"],
            bumped_list_price,
            float(before_row["discount_pct"]),
            bumped_final_price,
            before_row["currency"],
            "demo synthetic price increase (+$5.00)",
        )],
        schema="product_id string, seller_id string, category string, "
               "list_price double, discount_pct double, final_price double, "
               "currency string, change_reason string",
    )

    print(f"\n  Simulating an incoming price update for {product_id}: "
          f"list_price {before_row['list_price']} -> {bumped_list_price}")

    new_n, changed_n, unchanged_n = apply_scd_merge(spark, incoming)
    print(f"\n  Merge result: new={new_n} changed={changed_n} unchanged={unchanged_n}")

    show_product(spark, product_id, "AFTER")

    after_current = (
        spark.table(TARGET)
        .filter((col("product_id") == product_id) & (col("is_current") == True))
        .count()
    )
    print(f"\n  Current-row count for {product_id} after merge: {after_current} "
          f"({'✓ exactly one' if after_current == 1 else '✗ UNEXPECTED'})")

    print(f"\n{sep}\n")
    spark.stop()


if __name__ == "__main__":
    main()
