"""
data_quality.py
────────────────
Data quality checks across all three lakehouse layers.
Runs after gold_facts.py in the batch_pipeline DAG.

Checks performed
─────────────────
Bronze
  DQ-B1  orders            – no NULL order_id, row count > 0
  DQ-B2  product_catalog   – no NULL product_id, row count > 0
  DQ-B3  product_pricing   – no NULL pricing_sk, row count > 0
  DQ-B4  reviews           – no NULL review_id, row count > 0
  DQ-B5  user_events       – no NULL event_id, row count > 0

Silver
  DQ-S1  dim_product          – no duplicate product_id
  DQ-S2  dim_product_pricing  – every product has exactly one is_current=true row
  DQ-S3  dim_product_pricing  – no negative list_price

Gold
  DQ-G1  fact_orders          – no NULL order_id, row count > 0
  DQ-G2  fact_orders          – unit_price > 0 for all rows
  DQ-G3  fact_orders          – late-arrival lag ≤ 48 h (or is flagged)
  DQ-G4  fact_user_events     – no NULL event_id
  DQ-G5  ecommerce_summary    – no NULL summary_date
  DQ-G6  ml_session_conversion– converted column only contains 0 or 1
  DQ-G7  fact_orders          – no duplicate order_id (incremental merge integrity)
  DQ-G8  fact_orders          – arrival_time is never before event_time
  DQ-G9  fact_orders / orders_quarantine – order_id sets are disjoint
  DQ-S4  dim_product_pricing_scd – no duplicate pricing_sk
  DQ-S5  dim_product_pricing_scd – effective_to is consistent with is_current

Streaming (only run if the tables already exist — the streaming DAG runs on
its own hourly schedule independent of the batch DAG that runs this file)
  DQ-T1  user_events_stream        – no duplicate event_id
  DQ-T2  realtime_event_summary    – row count > 0

Exit code: 0 = all checks passed, 1 = at least one check failed.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        /jobs/data_quality.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when


def get_spark():
    return SparkSession.builder.appName("DataQuality").getOrCreate()


# ── Check helpers ──────────────────────────────────────────────────────────────

_results = []   # list of (check_id, passed, detail)


def check(check_id: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    mark   = "✓" if passed else "✗"
    print(f"  {mark} [{status}] {check_id}  {detail}")
    _results.append((check_id, passed, detail))


def no_nulls(df, pk_col: str, check_id: str):
    nulls = df.filter(col(pk_col).isNull()).count()
    check(check_id, nulls == 0, f"null {pk_col} = {nulls}")


def row_count_gt_zero(df, check_id: str):
    n = df.count()
    check(check_id, n > 0, f"rows = {n:,}")
    return n


# ── Bronze checks ──────────────────────────────────────────────────────────────

def check_bronze(spark):
    sep = "-" * 50
    print(f"\n  {sep}")
    print("  Bronze layer")
    print(f"  {sep}")

    specs = [
        ("demo.bronze.orders",          "order_id",   "DQ-B1"),
        ("demo.bronze.product_catalog", "product_id", "DQ-B2"),
        ("demo.bronze.product_pricing", "pricing_sk", "DQ-B3"),
        ("demo.bronze.reviews",         "review_id",  "DQ-B4"),
        ("demo.bronze.user_events",     "event_id",   "DQ-B5"),
    ]

    for table, pk, cid in specs:
        df = spark.table(table)
        n  = row_count_gt_zero(df, f"{cid}a row-count ({table})")
        if n > 0:
            no_nulls(df, pk, f"{cid}b null-pk  ({table})")


# ── Silver checks ──────────────────────────────────────────────────────────────

def check_silver(spark):
    sep = "-" * 50
    print(f"\n  {sep}")
    print("  Silver layer")
    print(f"  {sep}")

    # DQ-S1: no duplicate product_id in dim_product
    dim = spark.table("demo.silver.dim_product")
    dups = (
        dim.groupBy("product_id")
           .agg(count("*").alias("n"))
           .filter(col("n") > 1)
           .count()
    )
    check("DQ-S1", dups == 0, f"duplicate product_id rows = {dups}")

    # DQ-S2: every product has exactly one is_current=true pricing row
    pricing = spark.table("demo.silver.dim_product_pricing_scd")
    bad_current = (
        pricing.filter(col("is_current") == True)
               .groupBy("product_id")
               .agg(count("*").alias("n"))
               .filter(col("n") != 1)
               .count()
    )
    check("DQ-S2", bad_current == 0,
          f"products with != 1 current-price rows = {bad_current}")

    # DQ-S3: no negative list_price
    neg_price = pricing.filter(col("list_price") < 0).count()
    check("DQ-S3", neg_price == 0, f"rows with list_price < 0 = {neg_price}")

    # DQ-S4: no duplicate pricing_sk (SCD version rows must be uniquely keyed)
    dup_sk = (
        pricing.groupBy("pricing_sk").agg(count("*").alias("n"))
        .filter(col("n") > 1).count()
    )
    check("DQ-S4", dup_sk == 0, f"duplicate pricing_sk rows = {dup_sk}")

    # DQ-S5: effective_to must be consistent with is_current on every SCD row
    bad_scd = (
        pricing
        .filter(
            ((col("is_current") == True) & col("effective_to").isNotNull())
            | ((col("is_current") == False) & col("effective_to").isNull())
        )
        .count()
    )
    check("DQ-S5", bad_scd == 0, f"rows with inconsistent effective_to/is_current = {bad_scd}")


# ── Gold checks ────────────────────────────────────────────────────────────────

def check_gold(spark):
    sep = "-" * 50
    print(f"\n  {sep}")
    print("  Gold layer")
    print(f"  {sep}")

    # DQ-G1: fact_orders row count + no null order_id
    orders = spark.table("demo.gold.fact_orders")
    row_count_gt_zero(orders, "DQ-G1a row-count (fact_orders)")
    no_nulls(orders, "order_id", "DQ-G1b null-pk  (fact_orders)")

    # DQ-G2: unit_price > 0
    bad_price = orders.filter(col("unit_price") <= 0).count()
    check("DQ-G2", bad_price == 0, f"rows with unit_price <= 0 = {bad_price}")

    # DQ-G3: late-arrival rows flagged correctly
    # orders with arrival_lag_hours > 48 must have late_arrival_flag = true
    unflagged_late = (
        orders
        .filter(col("arrival_lag_hours") > 48)
        .filter(col("late_arrival_flag") != True)
        .count()
    )
    check("DQ-G3", unflagged_late == 0,
          f"late orders (>48 h) without flag = {unflagged_late}")

    # DQ-G4: fact_user_events no null event_id
    events = spark.table("demo.gold.fact_user_events")
    no_nulls(events, "event_id", "DQ-G4 null-pk (fact_user_events)")

    # DQ-G5: ecommerce_summary no null summary_date
    summary = spark.table("demo.gold.ecommerce_summary")
    null_dates = summary.filter(col("summary_date").isNull()).count()
    check("DQ-G5", null_dates == 0,
          f"rows with null summary_date = {null_dates}")

    # DQ-G6: ml_session_conversion converted column is binary (0 or 1)
    ml = spark.table("demo.gold.ml_session_conversion")
    bad_converted = ml.filter(~col("converted").isin(0, 1)).count()
    check("DQ-G6", bad_converted == 0,
          f"rows with converted not in {{0,1}} = {bad_converted}")

    # DQ-G7: no duplicate order_id (proves the incremental MERGE never
    # inserts a second row for an order it has already processed)
    dup_orders = (
        orders.groupBy("order_id").agg(count("*").alias("n"))
        .filter(col("n") > 1).count()
    )
    check("DQ-G7", dup_orders == 0, f"duplicate order_id rows = {dup_orders}")

    # DQ-G8: arrival_time must never be earlier than event_time
    bad_arrival = orders.filter(col("arrival_time") < col("event_time")).count()
    check("DQ-G8", bad_arrival == 0,
          f"rows with arrival_time < event_time = {bad_arrival}")

    # DQ-G9: fact_orders and orders_quarantine must never share an order_id
    if table_exists(spark, "demo.bronze.orders_quarantine"):
        quarantine = spark.table("demo.bronze.orders_quarantine")
        overlap = (
            orders.select("order_id")
            .join(quarantine.select("order_id"), on="order_id", how="inner")
            .count()
        )
        check("DQ-G9", overlap == 0,
              f"order_id present in both fact_orders and orders_quarantine = {overlap}")
    else:
        print("  - DQ-G9 skipped: demo.bronze.orders_quarantine does not exist yet "
              "(no late (>48h) orders have been quarantined)")


def table_exists(spark, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except Exception:
        return False


# ── Streaming checks (best-effort — the streaming DAG runs independently) ──────

def check_streaming(spark):
    sep = "-" * 50
    print(f"\n  {sep}")
    print("  Streaming layer (best-effort — skipped if not yet populated)")
    print(f"  {sep}")

    if not table_exists(spark, "demo.silver.user_events_stream_clean"):
        print("  - DQ-T1 skipped: demo.silver.user_events_stream_clean does not exist yet "
              "(streaming_pipeline DAG has not completed a cycle)")
    else:
        clean = spark.table("demo.silver.user_events_stream_clean")
        dup_events = (
            clean.groupBy("event_id").agg(count("*").alias("n"))
            .filter(col("n") > 1).count()
        )
        check("DQ-T1", dup_events == 0, f"duplicate event_id in stream clean = {dup_events}")

    if not table_exists(spark, "demo.gold.realtime_event_summary"):
        print("  - DQ-T2 skipped: demo.gold.realtime_event_summary does not exist yet "
              "(streaming_pipeline DAG has not completed a cycle)")
    else:
        n = spark.table("demo.gold.realtime_event_summary").count()
        check("DQ-T2", n > 0, f"rows = {n:,}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Data Quality Checks")
    print(f"{sep}")

    check_bronze(spark)
    check_silver(spark)
    check_gold(spark)
    check_streaming(spark)

    passed = sum(1 for _, ok, _ in _results if ok)
    failed = sum(1 for _, ok, _ in _results if not ok)
    total  = len(_results)

    print(f"\n{sep}")
    print(f"  Results: {passed}/{total} passed  |  {failed} failed")
    print(f"{sep}\n")

    spark.stop()

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
