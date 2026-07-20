"""
build_dashboard.py
───────────────────
Reads from the Gold Iceberg tables and writes a self-contained HTML
business dashboard to /jobs/dashboard.html (which maps to
processing/jobs/dashboard.html on the host via the Docker volume mount).

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/build_dashboard.py

Then open:  processing/jobs/dashboard.html  in your browser.
"""

import json
from pyspark.sql import SparkSession

OUTPUT_PATH = "/jobs/dashboard.html"


def get_spark():
    return SparkSession.builder.appName("BuildDashboard").getOrCreate()


def scalar(spark, sql):
    row = spark.sql(sql).collect()
    if row and row[0][0] is not None:
        return row[0][0]
    return 0


def table_exists(spark, table):
    try:
        spark.table(table)
        return True
    except Exception:
        return False


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Building Business Dashboard from Gold Layer")
    print(f"{sep}\n")

    # ── KPI scalars ───────────────────────────────────────────────────────────
    revenue = scalar(spark, """
        SELECT ROUND(SUM(total_amount), 2)
        FROM demo.gold.fact_orders
        WHERE payment_status = 'paid'
    """) or 0

    orders = scalar(spark, "SELECT COUNT(*) FROM demo.gold.fact_orders") or 0

    sessions = scalar(spark, """
        SELECT COUNT(DISTINCT session_id) FROM demo.gold.fact_user_events
    """) or 0

    converted = scalar(spark, """
        SELECT SUM(converted) FROM demo.gold.ml_session_conversion
    """) or 0

    avg_rating = scalar(spark, """
        SELECT ROUND(AVG(CAST(rating AS DOUBLE)), 2) FROM demo.bronze.reviews
    """) or 0

    late_count = scalar(spark, """
        SELECT COUNT(*) FROM demo.gold.fact_orders
        WHERE shipping_status = 'delayed'
    """) or 0

    conv_rate = round(converted / sessions * 100, 1) if sessions else 0
    late_rate = round(late_count / orders * 100, 1) if orders else 0
    stars = "★" * round(avg_rating) + "☆" * (5 - round(avg_rating))

    # ── Chart data ────────────────────────────────────────────────────────────
    trend_rows = spark.sql("""
        SELECT
            DATE_FORMAT(summary_date, 'yyyy-MM') AS month,
            ROUND(SUM(gross_revenue), 2)          AS revenue
        FROM demo.gold.ecommerce_summary
        WHERE gross_revenue IS NOT NULL
        GROUP BY DATE_FORMAT(summary_date, 'yyyy-MM')
        ORDER BY month
        LIMIT 12
    """).collect()

    cat_rows = spark.sql("""
        SELECT category, ROUND(SUM(gross_revenue), 2) AS revenue
        FROM demo.gold.ecommerce_summary
        WHERE gross_revenue IS NOT NULL AND category IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
        LIMIT 6
    """).collect()

    funnel_rows = spark.sql("""
        SELECT event_type, SUM(event_count) AS events
        FROM demo.gold.ecommerce_summary
        WHERE event_type IS NOT NULL
        GROUP BY event_type
        ORDER BY events DESC
    """).collect()

    fd = {r["event_type"]: int(r["events"] or 0) for r in funnel_rows}
    base = fd.get("product_view", 1) or 1
    funnel_stages = [
        ("Product Views",   "product_view"),
        ("Add to Cart",     "add_to_cart"),
        ("Purchase Clicks", "purchase_click"),
    ]
    funnel_values = [fd.get(stage[1], 0) for stage in funnel_stages]
    product_views, add_to_cart, purchase_clicks = funnel_values
    view_to_cart_loss = round((1 - add_to_cart / product_views) * 100, 1) if product_views else 0
    cart_to_purchase_loss = round((1 - purchase_clicks / add_to_cart) * 100, 1) if add_to_cart else 0
    biggest_drop = (
        "Product view to add-to-cart"
        if view_to_cart_loss >= cart_to_purchase_loss
        else "Add-to-cart to purchase click"
    )
    biggest_drop_rate = max(view_to_cart_loss, cart_to_purchase_loss)

    channel_rows = spark.sql("""
        SELECT
            traffic_channel,
            COUNT(*) AS sessions,
            SUM(converted) AS converted,
            ROUND(SUM(converted) * 100.0 / COUNT(*), 1) AS conversion_rate
        FROM demo.gold.ml_session_conversion
        WHERE traffic_channel IS NOT NULL
        GROUP BY traffic_channel
        HAVING sessions >= 100
        ORDER BY conversion_rate DESC, sessions DESC
        LIMIT 6
    """).collect()

    # ── Real-time stream KPIs (proves Kafka -> Bronze -> Silver -> Gold -> here) ─
    stream_available = table_exists(spark, "demo.gold.realtime_event_summary")
    if stream_available:
        stream_events = scalar(spark, "SELECT SUM(event_count) FROM demo.gold.realtime_event_summary") or 0
        stream_sessions = scalar(spark, "SELECT SUM(sessions) FROM demo.gold.realtime_event_summary") or 0
        stream_late = scalar(spark, "SELECT SUM(late_arrival_count) FROM demo.gold.realtime_event_summary") or 0
        stream_late_rate = round(stream_late / stream_events * 100, 1) if stream_events else 0
        stream_type_rows = spark.sql("""
            SELECT event_type, SUM(event_count) AS events
            FROM demo.gold.realtime_event_summary
            GROUP BY event_type
            ORDER BY events DESC
            LIMIT 6
        """).collect()
    else:
        stream_events = stream_sessions = stream_late = stream_late_rate = 0
        stream_type_rows = []

    stream_type_labels_js = json.dumps([str(r["event_type"]) for r in stream_type_rows])
    stream_type_counts_js = json.dumps([int(r["events"] or 0) for r in stream_type_rows])

    top_category = str(cat_rows[0]["category"]) if cat_rows else "N/A"
    top_category_revenue = float(cat_rows[0]["revenue"] or 0) if cat_rows else 0
    total_category_revenue = sum(float(r["revenue"] or 0) for r in cat_rows)
    top_category_share = round(top_category_revenue / total_category_revenue * 100, 1) if total_category_revenue else 0
    top_channel = str(channel_rows[0]["traffic_channel"]) if channel_rows else "N/A"
    top_channel_rate = float(channel_rows[0]["conversion_rate"] or 0) if channel_rows else 0

    trend_months_js  = json.dumps([str(r["month"])    for r in trend_rows])
    trend_revenue_js = json.dumps([float(r["revenue"] or 0) for r in trend_rows])
    cat_labels_js    = json.dumps([str(r["category"])  for r in cat_rows])
    cat_revenue_js   = json.dumps([float(r["revenue"] or 0) for r in cat_rows])
    funnel_labels_js = json.dumps([s[0] for s in funnel_stages])
    funnel_pcts_js   = json.dumps([round(fd.get(s[1], 0) / base * 100, 1) for s in funnel_stages])
    funnel_counts_js = json.dumps([int(fd.get(s[1], 0)) for s in funnel_stages])
    channel_labels_js = json.dumps([str(r["traffic_channel"]) for r in channel_rows])
    channel_rates_js = json.dumps([float(r["conversion_rate"] or 0) for r in channel_rows])
    channel_sessions_js = json.dumps([int(r["sessions"] or 0) for r in channel_rows])

    # ── Data quality summary from our DQ checks ───────────────────────────────
    dq_checks = [
        ("orders not empty",            orders > 0),
        ("sessions not empty",          sessions > 0),
        ("no null order ids",           scalar(spark, "SELECT COUNT(*) FROM demo.gold.fact_orders WHERE order_id IS NULL") == 0),
        ("unit price positive",         scalar(spark, "SELECT COUNT(*) FROM demo.gold.fact_orders WHERE unit_price <= 0") == 0),
        ("late arrivals flagged",       scalar(spark, "SELECT COUNT(*) FROM demo.gold.fact_orders WHERE arrival_lag_hours > 48 AND late_arrival_flag != true") == 0),
        ("no dup products",             scalar(spark, "SELECT COUNT(*) FROM (SELECT product_id, COUNT(*) AS n FROM demo.silver.dim_product GROUP BY product_id HAVING n > 1)") == 0),
        ("scd one current row",         scalar(spark, "SELECT COUNT(*) FROM (SELECT product_id, COUNT(*) AS n FROM demo.silver.dim_product_pricing_scd WHERE is_current=true GROUP BY product_id HAVING n != 1)") == 0),
        ("no negative prices",          scalar(spark, "SELECT COUNT(*) FROM demo.silver.dim_product_pricing_scd WHERE list_price < 0") == 0),
        ("conversion binary",           scalar(spark, "SELECT COUNT(*) FROM demo.gold.ml_session_conversion WHERE converted NOT IN (0,1)") == 0),
        ("summary dates not null",      scalar(spark, "SELECT COUNT(*) FROM demo.gold.ecommerce_summary WHERE summary_date IS NULL") == 0),
        ("no duplicate order ids",      scalar(spark, "SELECT COUNT(*) FROM (SELECT order_id, COUNT(*) AS n FROM demo.gold.fact_orders GROUP BY order_id HAVING n > 1)") == 0),
        ("streaming reached gold",      (not stream_available) or stream_events > 0),
    ]

    passed = sum(1 for _, ok in dq_checks if ok)
    failed = len(dq_checks) - passed
    badge_ok = failed == 0
    badge_txt = f"{passed}/{len(dq_checks)} quality checks passed"
    quality_chips = "".join(
        f'<span class="chip{"" if ok else " fail"}">{name}</span>'
        for name, ok in dq_checks
    )

    # ── CSS ───────────────────────────────────────────────────────────────────
    css = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body   { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f8; color: #1f2937; }
header {
  background: #17324d;
  color: white; padding: 22px 36px;
  display: flex; justify-content: space-between; align-items: center;
  gap: 24px;
}
header h1 { font-size: 22px; font-weight: 700; }
header p  { font-size: 13px; opacity: .82; margin-top: 4px; max-width: 820px; line-height: 1.45; }
.badge { font-size: 12px; padding: 6px 14px; border-radius: 20px; font-weight: 600; white-space: nowrap; }
main { padding: 24px 36px; }

.answer {
  display: grid; grid-template-columns: 1.4fr 1fr 1fr; gap: 14px; margin-bottom: 20px;
}
.answer .card { border-top: 3px solid #10b981; }
.answer .question {
  font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: .4px;
}
.answer .statement {
  font-size: 22px; line-height: 1.25; font-weight: 700; color: #17324d; margin-top: 8px;
}
.answer .detail { font-size: 12px; color: #6b7280; line-height: 1.45; margin-top: 8px; }

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

.row2 { display: grid; grid-template-columns: 3fr 2fr; gap: 14px; margin-bottom: 20px; }
.row3 { display: grid; grid-template-columns: 2fr 2fr 1.2fr; gap: 14px; margin-bottom: 20px; }
.card {
  background: white; border-radius: 10px; padding: 20px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}
.card h2 { font-size: 14px; font-weight: 600; color: #374151; margin-bottom: 16px; }

.drop-card { display: flex; flex-direction: column; justify-content: center; }
.drop-card .big { font-size: 34px; font-weight: 700; color: #d97706; }
.drop-card .label { font-size: 13px; color: #374151; margin-top: 8px; line-height: 1.35; }
.drop-card .sub { font-size: 12px; color: #6b7280; margin-top: 8px; line-height: 1.45; }
.donut-wrap   { position: relative; width: 100%; max-width: 180px; margin: 8px auto 0; }
.donut-center {
  position: absolute; top: 50%; left: 50%;
  transform: translate(-50%, -42%); text-align: center; pointer-events: none;
}
.donut-center .big { font-size: 22px; font-weight: 700; color: #f59e0b; }
.donut-center .sm  { font-size: 11px; color: #6b7280; }

.sat-card { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }
.stars    { font-size: 28px; color: #f59e0b; letter-spacing: 3px; margin: 10px 0 4px; }
.rnum     { font-size: 38px; font-weight: 700; color: #1e3a5f; margin-top: 12px; }
.rsub     { font-size: 12px; color: #9ca3af; margin-top: 6px; }

.quality {
  background: white; border-radius: 10px; padding: 14px 22px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
}
.quality h2 { font-size: 13px; font-weight: 600; color: #374151; margin-right: 4px; white-space: nowrap; }
.chip       { font-size: 11px; padding: 3px 9px; border-radius: 10px; background: #d1fae5; color: #065f46; }
.chip.fail  { background: #fee2e2; color: #991b1b; }
"""

    # ── JS charts ─────────────────────────────────────────────────────────────
    js_charts = """
new Chart(document.getElementById('trendChart'), {
  type: 'line',
  data: {
    labels: trendMonths,
    datasets: [{
      data: trendRevenue,
      borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,0.08)',
      borderWidth: 2.5, pointRadius: 4, pointBackgroundColor: '#2563eb',
      fill: true, tension: 0.4
    }]
  },
  options: {
    plugins: { legend: { display: false } },
    scales: {
      y: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'K' }, grid: { color: '#f3f4f6' } },
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
      x: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'K' }, grid: { color: '#f3f4f6' } },
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
      tooltip: { callbacks: { label: ctx => ' ' + funnelPcts[ctx.dataIndex] + '%  (' + funnelCounts[ctx.dataIndex].toLocaleString() + ' events)' } }
    },
    scales: {
      x: { max: 100, ticks: { callback: v => v + '%' }, grid: { color: '#f3f4f6' } },
      y: { grid: { display: false } }
    }
  }
});

if (document.getElementById('streamChart')) {
  new Chart(document.getElementById('streamChart'), {
    type: 'bar',
    data: {
      labels: streamTypeLabels,
      datasets: [{ data: streamTypeCounts, backgroundColor: '#8b5cf6', borderRadius: 4 }]
    },
    options: {
      indexAxis: 'y',
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: '#f3f4f6' } },
        y: { grid: { display: false } }
      }
    }
  });
}

new Chart(document.getElementById('channelChart'), {
  type: 'bar',
  data: {
    labels: channelLabels,
    datasets: [{ data: channelRates, backgroundColor: '#10b981', borderRadius: 4 }]
  },
  options: {
    indexAxis: 'y',
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { label: ctx => ' ' + channelRates[ctx.dataIndex] + '% conversion  (' + channelSessions[ctx.dataIndex].toLocaleString() + ' sessions)' } }
    },
    scales: {
      x: { max: 100, ticks: { callback: v => v + '%' }, grid: { color: '#f3f4f6' } },
      y: { grid: { display: false } }
    }
  }
});
"""

    badge_style = (
        "background:#d1fae5;color:#065f46;"
        if badge_ok else
        "background:#fee2e2;color:#991b1b;"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Amazon Marketplace — Business Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>{css}</style>
</head>
<body>

<header>
  <div>
    <h1>Revenue and Conversion Dashboard</h1>
    <p>Business question: Which product categories drive revenue, and where should the marketplace improve the purchase journey to increase conversion?</p>
  </div>
  <span class="badge" style="{badge_style}">{badge_txt}</span>
</header>

<main>

  <div class="answer">
    <div class="card">
      <div class="question">Dashboard answer</div>
      <div class="statement">{top_category} is the strongest revenue category, while the largest conversion loss is {biggest_drop.lower()}.</div>
      <div class="detail">The dashboard connects paid orders, category revenue, clickstream funnel behavior, and traffic-channel conversion to support one decision: where to focus growth effort.</div>
    </div>
    <div class="card">
      <div class="question">Best category</div>
      <div class="statement">{top_category}</div>
      <div class="detail">${top_category_revenue:,.0f} revenue across the top category set, representing {top_category_share}% of the displayed category revenue.</div>
    </div>
    <div class="card">
      <div class="question">Best channel</div>
      <div class="statement">{top_channel}</div>
      <div class="detail">{top_channel_rate}% session conversion rate among channels with meaningful traffic.</div>
    </div>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="label">Total Revenue</div>
      <div class="value">${revenue / 1_000_000:.2f}M</div>
      <div class="sub">paid orders only</div>
    </div>
    <div class="kpi">
      <div class="label">Total Orders</div>
      <div class="value">{orders:,}</div>
      <div class="sub">all payment statuses</div>
    </div>
    <div class="kpi green">
      <div class="label">Conversion Rate</div>
      <div class="value">{conv_rate}%</div>
      <div class="sub">{int(converted):,} of {sessions:,} sessions</div>
    </div>
    <div class="kpi amber">
      <div class="label">Biggest Funnel Loss</div>
      <div class="value">{biggest_drop_rate}%</div>
      <div class="sub">{biggest_drop}</div>
    </div>
    <div class="kpi purple">
      <div class="label">Avg Rating</div>
      <div class="value">{avg_rating}/5</div>
      <div class="sub">across all reviews</div>
    </div>
  </div>

  <div class="row2">
    <div class="card">
      <h2>Revenue Trend: Is the business growing?</h2>
      <canvas id="trendChart" height="85"></canvas>
    </div>
    <div class="card">
      <h2>Category Revenue: Where is money made?</h2>
      <canvas id="catChart" height="165"></canvas>
    </div>
  </div>

  <div class="row3">
    <div class="card">
      <h2>Purchase Funnel: Where do users drop?</h2>
      <canvas id="funnelChart" height="105"></canvas>
    </div>
    <div class="card">
      <h2>Channel Conversion: Which traffic performs best?</h2>
      <canvas id="channelChart" height="105"></canvas>
    </div>
    <div class="card drop-card">
      <h2>Action Point</h2>
      <div class="big">{biggest_drop_rate}%</div>
      <div class="label">{biggest_drop}</div>
      <div class="sub">This is the largest measured drop in the journey, so it is the clearest place to investigate UX, pricing, promotions, or checkout friction.</div>
    </div>
  </div>

  <div class="row2">
    <div class="card">
      <h2>Real-Time Stream Activity: Kafka &rarr; Bronze &rarr; Silver &rarr; Gold</h2>
      {"<canvas id='streamChart' height='105'></canvas>" if stream_available and stream_events else "<p style='color:#6b7280;font-size:13px;'>No streaming data yet — start the streaming stack and trigger the <code>streaming_pipeline</code> Airflow DAG, then re-run this job.</p>"}
    </div>
    <div class="card">
      <h2>Stream KPIs</h2>
      <div class="kpis" style="grid-template-columns: repeat(3, 1fr);">
        <div class="kpi">
          <div class="label">Streamed Events</div>
          <div class="value">{stream_events:,}</div>
          <div class="sub">via Kafka -&gt; Iceberg</div>
        </div>
        <div class="kpi">
          <div class="label">Streamed Sessions</div>
          <div class="value">{stream_sessions:,}</div>
          <div class="sub">distinct session_id</div>
        </div>
        <div class="kpi amber">
          <div class="label">Late Arrivals</div>
          <div class="value">{stream_late_rate}%</div>
          <div class="sub">{stream_late:,} events &gt; 60 min lag</div>
        </div>
      </div>
    </div>
  </div>

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
const channelLabels = {channel_labels_js};
const channelRates = {channel_rates_js};
const channelSessions = {channel_sessions_js};
const streamTypeLabels = {stream_type_labels_js};
const streamTypeCounts = {stream_type_counts_js};
{js_charts}
</script>

</body>
</html>"""

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  ✓ Dashboard written to {OUTPUT_PATH}")
    print(f"\n  KPIs:")
    print(f"    Revenue        : ${revenue/1_000_000:.2f}M")
    print(f"    Orders         : {orders:,}")
    print(f"    Conversion rate: {conv_rate}%")
    print(f"    Late deliveries: {late_count:,} ({late_rate}%)")
    print(f"    Avg rating     : {avg_rating}/5")
    print(f"    Stream events  : {stream_events:,} ({'available' if stream_available else 'not available yet'})")
    print(f"\n  Data quality   : {passed}/{len(dq_checks)} checks passed")
    print(f"\n  Open on host: processing/jobs/dashboard.html")
    print(f"\n{sep}")
    print("  ✓ Dashboard built successfully!")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
