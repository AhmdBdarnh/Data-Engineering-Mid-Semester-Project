import csv
import json
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "demo_outputs"
DB_PATH = OUTPUT_DIR / "amazon_midsemester_demo.sqlite"
DASHBOARD_PATH = OUTPUT_DIR / "dashboard.html"
QUALITY_PATH = OUTPUT_DIR / "quality_report.json"

RAW_FILES = {
    "bronze_user_activity_events": PROJECT_ROOT / "amazon_user_activity_streaming_events.csv",
    "bronze_orders_late_arrivals": PROJECT_ROOT / "amazon_orders_late_arrivals.csv",
    "bronze_product_catalog_static": PROJECT_ROOT / "amazon_product_catalog_static_dimension.csv",
    "bronze_product_pricing_scd_type2": PROJECT_ROOT / "amazon_product_pricing_scd_type2.csv",
    "bronze_reviews_batch_api": PROJECT_ROOT / "amazon_reviews_batch_api.csv",
}


def connect() -> sqlite3.Connection:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def load_csv(conn: sqlite3.Connection, table_name: str, path: Path) -> int:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(
            f'CREATE TABLE "{table_name}" ({", ".join(f""""{column}" TEXT""" for column in columns)})'
        )
        placeholders = ", ".join("?" for _ in columns)
        conn.executemany(
            f'INSERT INTO "{table_name}" VALUES ({placeholders})',
            ([row[column] for column in columns] for row in reader),
        )
    conn.commit()
    return conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]


def build_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS dim_product;
        CREATE TABLE dim_product AS
        SELECT DISTINCT
            product_id,
            product_name,
            category,
            subcategory,
            brand,
            seller_id,
            launch_date,
            CAST(base_price AS REAL) AS base_price,
            warehouse_id,
            CASE WHEN is_active = 'True' THEN 1 ELSE 0 END AS is_active,
            CAST(weight_kg AS REAL) AS weight_kg,
            CAST(product_rating_initial AS REAL) AS product_rating_initial
        FROM bronze_product_catalog_static;

        DROP TABLE IF EXISTS dim_category_static;
        CREATE TABLE dim_category_static AS
        SELECT
            'CAT_' || printf('%03d', ROW_NUMBER() OVER (ORDER BY category)) AS category_sk,
            category
        FROM (SELECT DISTINCT category FROM dim_product);

        DROP TABLE IF EXISTS dim_product_pricing_scd;
        CREATE TABLE dim_product_pricing_scd AS
        SELECT
            pricing_sk,
            product_id,
            seller_id,
            category,
            CAST(list_price AS REAL) AS list_price,
            CAST(discount_pct AS REAL) AS discount_pct,
            CAST(final_price AS REAL) AS final_price,
            currency,
            effective_from,
            NULLIF(effective_to, '') AS effective_to,
            CASE WHEN is_current = 'True' THEN 1 ELSE 0 END AS is_current,
            change_reason
        FROM bronze_product_pricing_scd_type2;

        DROP TABLE IF EXISTS dim_customer_anonymous;
        CREATE TABLE dim_customer_anonymous AS
        SELECT DISTINCT customer_id
        FROM (
            SELECT customer_id FROM bronze_user_activity_events
            UNION
            SELECT customer_id FROM bronze_orders_late_arrivals
            UNION
            SELECT customer_id FROM bronze_reviews_batch_api
        );

        DROP TABLE IF EXISTS dim_channel;
        CREATE TABLE dim_channel AS
        SELECT DISTINCT traffic_channel
        FROM bronze_user_activity_events;

        DROP TABLE IF EXISTS dim_date;
        CREATE TABLE dim_date AS
        SELECT DISTINCT
            date_day,
            CAST(strftime('%Y', date_day) AS INTEGER) AS year,
            CAST(strftime('%m', date_day) AS INTEGER) AS month,
            CAST(strftime('%d', date_day) AS INTEGER) AS day
        FROM (
            SELECT substr(event_time, 1, 10) AS date_day FROM bronze_user_activity_events
            UNION
            SELECT substr(event_time, 1, 10) AS date_day FROM bronze_orders_late_arrivals
            UNION
            SELECT substr(review_time, 1, 10) AS date_day FROM bronze_reviews_batch_api
        );

        DROP TABLE IF EXISTS fact_user_event;
        CREATE TABLE fact_user_event AS
        SELECT
            event_id,
            event_time,
            substr(event_time, 1, 10) AS event_date,
            customer_id,
            session_id,
            event_type,
            product_id,
            device_type,
            traffic_channel,
            region,
            CAST(quantity AS INTEGER) AS quantity,
            user_agent_family,
            ingestion_time,
            ROUND((julianday(ingestion_time) - julianday(event_time)) * 24 * 60, 2) AS ingestion_lag_minutes
        FROM bronze_user_activity_events;

        DROP TABLE IF EXISTS fact_order;
        CREATE TABLE fact_order AS
        SELECT
            order_id,
            event_time,
            substr(event_time, 1, 10) AS order_date,
            arrival_time,
            customer_id,
            product_id,
            CAST(quantity AS INTEGER) AS quantity,
            CAST(unit_price AS REAL) AS unit_price,
            CAST(discount_amount AS REAL) AS discount_amount,
            CAST(shipping_fee AS REAL) AS shipping_fee,
            payment_status,
            shipping_status,
            warehouse_id,
            delivery_partner,
            CASE WHEN late_arrival_flag = 'True' THEN 1 ELSE 0 END AS late_arrival_flag,
            source_system,
            CAST(total_amount AS REAL) AS total_amount,
            ROUND((julianday(arrival_time) - julianday(event_time)) * 24, 2) AS arrival_lag_hours
        FROM bronze_orders_late_arrivals;

        DROP TABLE IF EXISTS fact_review;
        CREATE TABLE fact_review AS
        SELECT
            review_id,
            review_time,
            substr(review_time, 1, 10) AS review_date,
            customer_id,
            product_id,
            CAST(rating AS INTEGER) AS rating,
            review_title,
            CASE WHEN verified_purchase = 'True' THEN 1 ELSE 0 END AS verified_purchase,
            CAST(helpful_votes AS INTEGER) AS helpful_votes,
            CAST(sentiment_score AS REAL) AS sentiment_score,
            source_file_date,
            batch_loaded_at,
            ROUND(julianday(batch_loaded_at) - julianday(review_time), 2) AS batch_lag_days
        FROM bronze_reviews_batch_api;

        DROP TABLE IF EXISTS gold_daily_sales_summary;
        CREATE TABLE gold_daily_sales_summary AS
        SELECT
            o.order_date,
            p.category,
            p.subcategory,
            p.brand,
            o.shipping_status,
            COUNT(DISTINCT o.order_id) AS orders,
            SUM(o.quantity) AS units,
            ROUND(SUM(o.total_amount), 2) AS gross_revenue,
            ROUND(SUM(o.discount_amount), 2) AS total_discount,
            ROUND(AVG(o.arrival_lag_hours), 2) AS avg_arrival_lag_hours,
            SUM(o.late_arrival_flag) AS late_arrival_orders
        FROM fact_order o
        LEFT JOIN dim_product p ON o.product_id = p.product_id
        GROUP BY o.order_date, p.category, p.subcategory, p.brand, o.shipping_status;

        DROP TABLE IF EXISTS gold_conversion_funnel;
        CREATE TABLE gold_conversion_funnel AS
        SELECT
            event_date,
            traffic_channel,
            device_type,
            region,
            event_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT session_id) AS sessions
        FROM fact_user_event
        GROUP BY event_date, traffic_channel, device_type, region, event_type;

        DROP TABLE IF EXISTS gold_review_satisfaction_summary;
        CREATE TABLE gold_review_satisfaction_summary AS
        SELECT
            r.review_date,
            p.category,
            p.brand,
            COUNT(*) AS reviews,
            ROUND(AVG(r.rating), 2) AS avg_rating,
            ROUND(AVG(r.sentiment_score), 3) AS avg_sentiment,
            SUM(r.verified_purchase) AS verified_reviews
        FROM fact_review r
        LEFT JOIN dim_product p ON r.product_id = p.product_id
        GROUP BY r.review_date, p.category, p.brand;

        DROP TABLE IF EXISTS ml_session_conversion_features;
        CREATE TABLE ml_session_conversion_features AS
        SELECT
            session_id,
            customer_id,
            MIN(event_time) AS session_start_time,
            MAX(event_time) AS session_end_time,
            MIN(traffic_channel) AS traffic_channel,
            MIN(device_type) AS device_type,
            MIN(region) AS region,
            COUNT(*) AS event_count,
            COUNT(DISTINCT product_id) AS distinct_products_viewed,
            SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
            SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_events,
            MAX(CASE WHEN event_type = 'purchase_click' THEN 1 ELSE 0 END) AS converted
        FROM fact_user_event
        GROUP BY session_id, customer_id;
        """
    )
    conn.commit()


def scalar(conn: sqlite3.Connection, sql: str):
    return conn.execute(sql).fetchone()[0]


def quality_report(conn: sqlite3.Connection) -> dict:
    checks = [
        ("orders_not_empty", scalar(conn, "SELECT COUNT(*) FROM fact_order") > 0, "orders exist"),
        ("events_not_empty", scalar(conn, "SELECT COUNT(*) FROM fact_user_event") > 0, "events exist"),
        ("reviews_not_empty", scalar(conn, "SELECT COUNT(*) FROM fact_review") > 0, "reviews exist"),
        (
            "product_catalog_expected_rows",
            scalar(conn, "SELECT COUNT(*) FROM dim_product") == 12000,
            f"rows={scalar(conn, 'SELECT COUNT(*) FROM dim_product')}",
        ),
        (
            "orders_required_keys_not_null",
            scalar(
                conn,
                """
                SELECT COUNT(*) FROM fact_order
                WHERE order_id = '' OR customer_id = '' OR product_id = ''
                """,
            )
            == 0,
            "no missing order keys",
        ),
        (
            "orders_total_amount_non_negative",
            scalar(conn, "SELECT COUNT(*) FROM fact_order WHERE total_amount < 0") == 0,
            "no negative order totals",
        ),
        (
            "late_arrivals_within_48_hours",
            scalar(conn, "SELECT COUNT(*) FROM fact_order WHERE arrival_lag_hours > 48.0") == 0,
            "all late arrivals within 48 hours",
        ),
        (
            "one_current_scd_row_per_product",
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT product_id, SUM(is_current) AS current_rows
                    FROM dim_product_pricing_scd
                    GROUP BY product_id
                    HAVING current_rows != 1
                )
                """,
            )
            == 0,
            "one current pricing row per product",
        ),
        (
            "review_rating_between_1_and_5",
            scalar(conn, "SELECT COUNT(*) FROM fact_review WHERE rating NOT BETWEEN 1 AND 5") == 0,
            "ratings are valid",
        ),
    ]
    results = [{"check": name, "status": "passed" if ok else "failed", "details": details} for name, ok, details in checks]
    failed = sum(1 for result in results if result["status"] == "failed")
    report = {"summary": {"checks": len(results), "passed": len(results) - failed, "failed": failed}, "checks": results}
    QUALITY_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def query_rows(conn: sqlite3.Connection, sql: str, limit: int = 10) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(f"{sql} LIMIT {limit}").fetchall()


def html_table(title: str, records: list[sqlite3.Row]) -> str:
    if not records:
        return f"<section><h2>{title}</h2><p>No rows.</p></section>"
    columns = records[0].keys()
    header = "".join(f"<th>{column}</th>" for column in columns)
    body = "".join("<tr>" + "".join(f"<td>{row[column]}</td>" for column in columns) + "</tr>" for row in records)
    return f"<section><h2>{title}</h2><table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></section>"


def build_dashboard(conn: sqlite3.Connection, report: dict) -> None:
    revenue = scalar(conn, "SELECT ROUND(SUM(total_amount), 2) FROM fact_order WHERE payment_status = 'paid'")
    orders = scalar(conn, "SELECT COUNT(*) FROM fact_order")
    sessions = scalar(conn, "SELECT COUNT(DISTINCT session_id) FROM fact_user_event")
    converted = scalar(conn, "SELECT SUM(converted) FROM ml_session_conversion_features")
    avg_rating = scalar(conn, "SELECT ROUND(AVG(rating), 2) FROM fact_review")

    top_categories = query_rows(
        conn,
        """
        SELECT category, SUM(orders) AS orders, ROUND(SUM(gross_revenue), 2) AS revenue
        FROM gold_daily_sales_summary
        GROUP BY category
        ORDER BY revenue DESC
        """,
        8,
    )
    funnel = query_rows(
        conn,
        """
        SELECT event_type, SUM(event_count) AS events, SUM(sessions) AS sessions
        FROM gold_conversion_funnel
        GROUP BY event_type
        ORDER BY events DESC
        """,
        10,
    )
    late_view = query_rows(
        conn,
        """
        SELECT shipping_status, COUNT(*) AS orders, ROUND(AVG(arrival_lag_hours), 2) AS avg_arrival_lag_hours
        FROM fact_order
        GROUP BY shipping_status
        ORDER BY orders DESC
        """,
        10,
    )
    reviews = query_rows(
        conn,
        """
        SELECT category, ROUND(AVG(avg_rating), 2) AS avg_rating, ROUND(AVG(avg_sentiment), 3) AS avg_sentiment
        FROM gold_review_satisfaction_summary
        GROUP BY category
        ORDER BY avg_rating DESC
        """,
        8,
    )

    badge = "passed" if report["summary"]["failed"] == 0 else "failed"
    DASHBOARD_PATH.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Amazon Ecommerce Mid-Semester Demo</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; background: #f6f7fb; color: #1f2937; }}
    header {{ background: #111827; color: white; padding: 24px 36px; }}
    main {{ padding: 24px 36px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); gap: 12px; margin-bottom: 24px; }}
    .metric, section {{ background: white; border: 1px solid #d8dee9; border-radius: 6px; padding: 16px; }}
    .metric strong {{ display: block; font-size: 22px; margin-top: 6px; }}
    section {{ margin-bottom: 18px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px 10px; text-align: left; }}
    th {{ background: #f3f4f6; }}
    .passed {{ color: #047857; font-weight: bold; }}
    .failed {{ color: #b91c1c; font-weight: bold; }}
  </style>
</head>
<body>
  <header>
    <h1>Amazon Ecommerce Mid-Semester Demo</h1>
    <p>CSV files to SQLite warehouse to dashboard and quality report.</p>
  </header>
  <main>
    <div class="metrics">
      <div class="metric">Paid Revenue<strong>${revenue:,.2f}</strong></div>
      <div class="metric">Orders<strong>{orders:,}</strong></div>
      <div class="metric">Sessions<strong>{sessions:,}</strong></div>
      <div class="metric">Converted Sessions<strong>{converted:,}</strong></div>
      <div class="metric">Avg Review Rating<strong>{avg_rating}</strong></div>
    </div>
    <section><h2>Quality Status</h2><p class="{badge}">{report["summary"]["passed"]}/{report["summary"]["checks"]} checks passed</p></section>
    {html_table("Top Revenue Categories", top_categories)}
    {html_table("Conversion Funnel", funnel)}
    {html_table("Shipping / Late Arrival View", late_view)}
    {html_table("Review Satisfaction by Category", reviews)}
  </main>
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> None:
    conn = connect()
    counts = {table: load_csv(conn, table, path) for table, path in RAW_FILES.items()}
    build_tables(conn)
    report = quality_report(conn)
    build_dashboard(conn, report)
    conn.close()

    print("Mid-semester demo built successfully.")
    print(f"SQLite warehouse: {DB_PATH}")
    print(f"Dashboard HTML:    {DASHBOARD_PATH}")
    print(f"Quality report:    {QUALITY_PATH}")
    print("Loaded bronze rows:")
    for table, count in counts.items():
        print(f"  {table}: {count}")
    print(f"Quality: {report['summary']['passed']}/{report['summary']['checks']} passed")


if __name__ == "__main__":
    main()
