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
        -- ── SILVER: reference dimensions ──────────────────────────────────────

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

        -- ── GOLD: fact tables ──────────────────────────────────────────────────

        DROP TABLE IF EXISTS fact_orders;
        CREATE TABLE fact_orders AS
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

        DROP TABLE IF EXISTS fact_user_events;
        CREATE TABLE fact_user_events AS
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

        -- ── GOLD: pre-aggregated dashboard summary ─────────────────────────────
        -- Three row types (NULL where metric does not apply to that row type):
        --   sales rows  → date + category + brand + shipping_status
        --   event rows  → date + traffic_channel + device_type + event_type
        --   review rows → date + category + brand

        DROP TABLE IF EXISTS gold_ecommerce_summary;
        CREATE TABLE gold_ecommerce_summary AS

        SELECT
            o.order_date        AS summary_date,
            p.category,
            p.subcategory,
            p.brand,
            o.shipping_status,
            NULL                AS traffic_channel,
            NULL                AS device_type,
            NULL                AS event_type,
            COUNT(DISTINCT o.order_id)         AS orders,
            SUM(o.quantity)                    AS units_sold,
            ROUND(SUM(o.total_amount), 2)      AS gross_revenue,
            ROUND(SUM(o.discount_amount), 2)   AS total_discount,
            ROUND(AVG(o.arrival_lag_hours), 2) AS avg_arrival_lag_hours,
            SUM(o.late_arrival_flag)           AS late_arrival_orders,
            NULL AS event_count,
            NULL AS sessions,
            NULL AS reviews,
            NULL AS avg_rating,
            NULL AS avg_sentiment
        FROM fact_orders o
        LEFT JOIN dim_product p ON o.product_id = p.product_id
        GROUP BY o.order_date, p.category, p.subcategory, p.brand, o.shipping_status

        UNION ALL

        SELECT
            event_date          AS summary_date,
            NULL                AS category,
            NULL                AS subcategory,
            NULL                AS brand,
            NULL                AS shipping_status,
            traffic_channel,
            device_type,
            event_type,
            NULL AS orders,
            NULL AS units_sold,
            NULL AS gross_revenue,
            NULL AS total_discount,
            NULL AS avg_arrival_lag_hours,
            NULL AS late_arrival_orders,
            COUNT(*)                   AS event_count,
            COUNT(DISTINCT session_id) AS sessions,
            NULL AS reviews,
            NULL AS avg_rating,
            NULL AS avg_sentiment
        FROM fact_user_events
        GROUP BY event_date, traffic_channel, device_type, event_type

        UNION ALL

        SELECT
            substr(r.review_time, 1, 10)   AS summary_date,
            p.category,
            p.subcategory,
            p.brand,
            NULL                AS shipping_status,
            NULL                AS traffic_channel,
            NULL                AS device_type,
            NULL                AS event_type,
            NULL AS orders,
            NULL AS units_sold,
            NULL AS gross_revenue,
            NULL AS total_discount,
            NULL AS avg_arrival_lag_hours,
            NULL AS late_arrival_orders,
            NULL AS event_count,
            NULL AS sessions,
            COUNT(*)                                       AS reviews,
            ROUND(AVG(CAST(r.rating AS REAL)), 2)          AS avg_rating,
            ROUND(AVG(CAST(r.sentiment_score AS REAL)), 3) AS avg_sentiment
        FROM bronze_reviews_batch_api r
        LEFT JOIN dim_product p ON r.product_id = p.product_id
        GROUP BY substr(r.review_time, 1, 10), p.category, p.subcategory, p.brand;

        -- ── GOLD: ML feature table ─────────────────────────────────────────────

        DROP TABLE IF EXISTS ml_session_conversion_features;
        CREATE TABLE ml_session_conversion_features AS
        SELECT
            session_id,
            customer_id,
            MIN(event_time)            AS session_start_time,
            MAX(event_time)            AS session_end_time,
            MIN(traffic_channel)       AS traffic_channel,
            MIN(device_type)           AS device_type,
            MIN(region)                AS region,
            COUNT(*)                   AS event_count,
            COUNT(DISTINCT product_id) AS distinct_products_viewed,
            SUM(CASE WHEN event_type = 'product_view'     THEN 1 ELSE 0 END) AS product_views,
            SUM(CASE WHEN event_type = 'add_to_cart'      THEN 1 ELSE 0 END) AS add_to_cart_events,
            SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_events,
            MAX(CASE WHEN event_type = 'purchase_click'   THEN 1 ELSE 0 END) AS converted
        FROM fact_user_events
        GROUP BY session_id, customer_id;
        """
    )
    conn.commit()


def scalar(conn: sqlite3.Connection, sql: str):
    return conn.execute(sql).fetchone()[0]


def quality_report(conn: sqlite3.Connection) -> dict:
    checks = [
        ("orders_not_empty", scalar(conn, "SELECT COUNT(*) FROM fact_orders") > 0, "orders exist"),
        ("events_not_empty", scalar(conn, "SELECT COUNT(*) FROM fact_user_events") > 0, "events exist"),
        ("reviews_not_empty", scalar(conn, "SELECT COUNT(*) FROM bronze_reviews_batch_api") > 0, "reviews exist"),
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
                SELECT COUNT(*) FROM fact_orders
                WHERE order_id = '' OR customer_id = '' OR product_id = ''
                """,
            )
            == 0,
            "no missing order keys",
        ),
        (
            "orders_total_amount_non_negative",
            scalar(conn, "SELECT COUNT(*) FROM fact_orders WHERE total_amount < 0") == 0,
            "no negative order totals",
        ),
        (
            "late_arrivals_within_48_hours",
            scalar(conn, "SELECT COUNT(*) FROM fact_orders WHERE arrival_lag_hours > 48.0") == 0,
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
            scalar(conn, "SELECT COUNT(*) FROM bronze_reviews_batch_api WHERE CAST(rating AS INTEGER) NOT BETWEEN 1 AND 5") == 0,
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


def build_dashboard(conn: sqlite3.Connection, report: dict) -> None:
    # ── Scalar KPIs ───────────────────────────────────────────────────────────
    revenue    = scalar(conn, "SELECT ROUND(SUM(total_amount), 2) FROM fact_orders WHERE payment_status = 'paid'") or 0
    orders     = scalar(conn, "SELECT COUNT(*) FROM fact_orders") or 0
    sessions   = scalar(conn, "SELECT COUNT(DISTINCT session_id) FROM fact_user_events") or 0
    converted  = scalar(conn, "SELECT SUM(converted) FROM ml_session_conversion_features") or 0
    avg_rating = scalar(conn, "SELECT ROUND(AVG(CAST(rating AS REAL)), 2) FROM bronze_reviews_batch_api") or 0
    late_count = scalar(conn, "SELECT COUNT(*) FROM fact_orders WHERE shipping_status = 'delayed'") or 0

    conv_rate = round(converted / sessions * 100, 1) if sessions else 0
    late_rate = round(late_count / orders * 100, 1) if orders else 0
    stars     = "★" * round(avg_rating) + "☆" * (5 - round(avg_rating))

    # ── Chart data queries ────────────────────────────────────────────────────
    trend_rows = query_rows(conn, """
        SELECT substr(summary_date, 1, 7) AS month,
               ROUND(SUM(gross_revenue), 2) AS revenue
        FROM gold_ecommerce_summary
        WHERE gross_revenue IS NOT NULL
        GROUP BY month ORDER BY month
    """, 12)

    cat_rows = query_rows(conn, """
        SELECT category, ROUND(SUM(gross_revenue), 2) AS revenue
        FROM gold_ecommerce_summary
        WHERE gross_revenue IS NOT NULL AND category IS NOT NULL
        GROUP BY category ORDER BY revenue DESC
    """, 6)

    funnel_rows = query_rows(conn, """
        SELECT event_type, SUM(event_count) AS events
        FROM gold_ecommerce_summary
        WHERE event_type IS NOT NULL
        GROUP BY event_type ORDER BY events DESC
    """, 20)

    fd   = {r["event_type"]: (r["events"] or 0) for r in funnel_rows}
    base = fd.get("product_view", 1) or 1
    funnel_stages = [
        ("Product Views",   "product_view"),
        ("Add to Cart",     "add_to_cart"),
        ("Purchase Clicks", "purchase_click"),
    ]

    # ── Serialise chart data as JSON for JS ───────────────────────────────────
    trend_months_js  = json.dumps([r["month"]   for r in trend_rows])
    trend_revenue_js = json.dumps([r["revenue"] for r in trend_rows])
    cat_labels_js    = json.dumps([r["category"] for r in cat_rows])
    cat_revenue_js   = json.dumps([r["revenue"]  for r in cat_rows])
    funnel_labels_js = json.dumps([s[0] for s in funnel_stages])
    funnel_pcts_js   = json.dumps([round(fd.get(s[1], 0) / base * 100, 1) for s in funnel_stages])
    funnel_counts_js = json.dumps([fd.get(s[1], 0) for s in funnel_stages])

    # ── Quality chips ─────────────────────────────────────────────────────────
    quality_chips = "".join(
        f'<span class="chip{"" if c["status"] == "passed" else " fail"}">'
        f'{c["check"].replace("_", " ")}</span>'
        for c in report["checks"]
    )
    badge_ok  = report["summary"]["failed"] == 0
    badge_txt = f'{report["summary"]["passed"]}/{report["summary"]["checks"]} quality checks passed'

    # ── CSS (plain string — real braces, no f-string) ─────────────────────────
    css = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body   { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f8; color: #1f2937; }
header {
  background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%);
  color: white; padding: 20px 36px;
  display: flex; justify-content: space-between; align-items: center;
}
header h1 { font-size: 22px; font-weight: 700; }
header p  { font-size: 13px; opacity: .8; margin-top: 4px; }
.badge    {
  font-size: 12px; padding: 6px 14px; border-radius: 20px; font-weight: 600;
}
main { padding: 24px 36px; }

/* KPI row */
.kpis { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-bottom: 20px; }
.kpi  {
  background: white; border-radius: 10px; padding: 18px 20px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08); border-top: 3px solid #2563eb;
}
.kpi .label { font-size: 11px; color: #6b7280; text-transform: uppercase; letter-spacing: .5px; }
.kpi .value { font-size: 26px; font-weight: 700; color: #1e3a5f; margin-top: 6px; }
.kpi .sub   { font-size: 11px; color: #9ca3af; margin-top: 3px; }
.kpi.green  { border-top-color: #10b981; }
.kpi.green .value { color: #059669; }
.kpi.amber  { border-top-color: #f59e0b; }
.kpi.amber .value { color: #d97706; }
.kpi.purple { border-top-color: #8b5cf6; }
.kpi.purple .value { color: #7c3aed; }

/* Chart layout */
.row2 { display: grid; grid-template-columns: 3fr 2fr; gap: 14px; margin-bottom: 20px; }
.row3 { display: grid; grid-template-columns: 2fr 1.4fr 1.4fr; gap: 14px; margin-bottom: 20px; }
.card {
  background: white; border-radius: 10px; padding: 20px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}
.card h2 { font-size: 14px; font-weight: 600; color: #374151; margin-bottom: 16px; }

/* Delivery donut */
.donut-wrap   { position: relative; width: 100%; max-width: 180px; margin: 8px auto 0; }
.donut-center {
  position: absolute; top: 50%; left: 50%;
  transform: translate(-50%, -42%); text-align: center; pointer-events: none;
}
.donut-center .big { font-size: 22px; font-weight: 700; color: #f59e0b; }
.donut-center .sm  { font-size: 11px; color: #6b7280; }

/* Satisfaction */
.sat-card { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }
.stars    { font-size: 28px; color: #f59e0b; letter-spacing: 3px; margin: 10px 0 4px; }
.rnum     { font-size: 38px; font-weight: 700; color: #1e3a5f; margin-top: 12px; }
.rsub     { font-size: 12px; color: #9ca3af; margin-top: 6px; }

/* Quality bar */
.quality {
  background: white; border-radius: 10px; padding: 14px 22px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
}
.quality h2 { font-size: 13px; font-weight: 600; color: #374151; margin-right: 4px; white-space: nowrap; }
.chip       { font-size: 11px; padding: 3px 9px; border-radius: 10px; background: #d1fae5; color: #065f46; }
.chip.fail  { background: #fee2e2; color: #991b1b; }
"""

    # ── Chart.js config (plain string — real braces) ───────────────────────────
    js_charts = """
new Chart(document.getElementById('trendChart'), {
  type: 'line',
  data: {
    labels: trendMonths,
    datasets: [{
      data: trendRevenue,
      borderColor: '#2563eb',
      backgroundColor: 'rgba(37,99,235,0.08)',
      borderWidth: 2.5, pointRadius: 4,
      pointBackgroundColor: '#2563eb',
      fill: true, tension: 0.4
    }]
  },
  options: {
    plugins: { legend: { display: false } },
    scales: {
      y: {
        ticks: { callback: v => '$' + (v / 1000).toFixed(0) + 'K' },
        grid: { color: '#f3f4f6' }
      },
      x: { grid: { display: false } }
    }
  }
});

new Chart(document.getElementById('catChart'), {
  type: 'bar',
  data: {
    labels: catLabels,
    datasets: [{
      data: catRevenue,
      backgroundColor: ['#1e3a5f','#2563eb','#3b82f6','#60a5fa','#93c5fd','#bfdbfe'],
      borderRadius: 4
    }]
  },
  options: {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: {
      x: {
        ticks: { callback: v => '$' + (v / 1000).toFixed(0) + 'K' },
        grid: { color: '#f3f4f6' }
      },
      y: { grid: { display: false } }
    }
  }
});

new Chart(document.getElementById('funnelChart'), {
  type: 'bar',
  data: {
    labels: funnelLabels,
    datasets: [{
      data: funnelPcts,
      backgroundColor: ['#2563eb','#3b82f6','#10b981'],
      borderRadius: 4
    }]
  },
  options: {
    indexAxis: 'y',
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: ctx =>
            ' ' + funnelPcts[ctx.dataIndex] + '%' +
            '  (' + funnelCounts[ctx.dataIndex].toLocaleString() + ' events)'
        }
      }
    },
    scales: {
      x: {
        max: 100,
        ticks: { callback: v => v + '%' },
        grid: { color: '#f3f4f6' }
      },
      y: { grid: { display: false } }
    }
  }
});

new Chart(document.getElementById('donutChart'), {
  type: 'doughnut',
  data: {
    datasets: [{
      data: [lateRate, 100 - lateRate],
      backgroundColor: ['#f59e0b', '#e5e7eb'],
      borderWidth: 0
    }]
  },
  options: {
    cutout: '72%',
    plugins: { legend: { display: false }, tooltip: { enabled: false } }
  }
});
"""

    # ── Assemble HTML (f-string only for Python values) ───────────────────────
    badge_style = (
        "background:#d1fae5;color:#065f46;" if badge_ok
        else "background:#fee2e2;color:#991b1b;"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Amazon Marketplace Business Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>{css}</style>
</head>
<body>

<header>
  <div>
    <h1>Amazon Marketplace Business Dashboard</h1>
    <p>Sales performance &bull; User behavior &bull; Delivery issues &bull; Customer satisfaction</p>
  </div>
  <span class="badge" style="{badge_style}">{badge_txt}</span>
</header>

<main>

  <!-- KPI cards -->
  <div class="kpis">
    <div class="kpi">
      <div class="label">Total Revenue</div>
      <div class="value">${revenue / 1_000_000:.2f}M</div>
      <div class="sub">paid orders only</div>
    </div>
    <div class="kpi">
      <div class="label">Number of Orders</div>
      <div class="value">{orders:,}</div>
      <div class="sub">all payment statuses</div>
    </div>
    <div class="kpi green">
      <div class="label">Conversion Rate</div>
      <div class="value">{conv_rate}%</div>
      <div class="sub">{converted:,} of {sessions:,} sessions</div>
    </div>
    <div class="kpi amber">
      <div class="label">Late Deliveries</div>
      <div class="value">{late_count:,}</div>
      <div class="sub">{late_rate}% of all orders</div>
    </div>
    <div class="kpi purple">
      <div class="label">Average Rating</div>
      <div class="value">{avg_rating}/5</div>
      <div class="sub">across all reviews</div>
    </div>
  </div>

  <!-- Row 2: trend + categories -->
  <div class="row2">
    <div class="card">
      <h2>Revenue Trend</h2>
      <canvas id="trendChart" height="85"></canvas>
    </div>
    <div class="card">
      <h2>Top Categories by Revenue</h2>
      <canvas id="catChart" height="165"></canvas>
    </div>
  </div>

  <!-- Row 3: funnel + delivery + satisfaction -->
  <div class="row3">
    <div class="card">
      <h2>Conversion Funnel</h2>
      <canvas id="funnelChart" height="105"></canvas>
    </div>
    <div class="card">
      <h2>Delivery Performance</h2>
      <div class="donut-wrap">
        <canvas id="donutChart"></canvas>
        <div class="donut-center">
          <div class="big">{late_rate}%</div>
          <div class="sm">late delivery rate</div>
        </div>
      </div>
    </div>
    <div class="card sat-card">
      <h2 style="align-self:flex-start;">Customer Satisfaction</h2>
      <div class="rnum">{avg_rating}</div>
      <div class="stars">{stars}</div>
      <div class="rsub">Average rating: {avg_rating}/5<br>Based on product reviews and ratings</div>
    </div>
  </div>

  <!-- Quality checks -->
  <div class="quality">
    <h2>Data Quality &mdash;</h2>
    {quality_chips}
  </div>

</main>

<script>
const trendMonths  = {trend_months_js};
const trendRevenue = {trend_revenue_js};
const catLabels    = {cat_labels_js};
const catRevenue   = {cat_revenue_js};
const funnelLabels = {funnel_labels_js};
const funnelPcts   = {funnel_pcts_js};
const funnelCounts = {funnel_counts_js};
const lateRate     = {late_rate};
{js_charts}
</script>

</body>
</html>"""

    DASHBOARD_PATH.write_text(html, encoding="utf-8")


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
