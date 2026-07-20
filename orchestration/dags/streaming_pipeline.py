"""
streaming_pipeline.py
──────────────────────
Hourly DAG that ensures the Kafka → Iceberg streaming consumer is alive
and that rows are landing in demo.bronze.user_events_stream.

Tasks
─────
1. start_streaming_consumer
   Launches streaming_consumer.py as a detached background process inside
   spark-master.  Idempotent via a PID file (/tmp/streaming_consumer.pid):
   does nothing if that PID is still alive and not a zombie. Liveness is
   checked with `ps -o stat= -p <pid>` and rejecting a leading "Z" state,
   because two simpler checks were tried and found unreliable in this
   container:
     - `kill -0 <pid>` returns success for a defunct/zombie PID too (a
       dead process whose exit status its parent hasn't reaped yet still
       occupies its PID slot), which caused a false "already running"
       report after the consumer had actually crashed.
     - a non-empty `/proc/<pid>/cmdline` check also proved unreliable:
       the java process behind streaming_consumer.py can show an EMPTY
       /proc/<pid>/cmdline while still genuinely alive (an OpenJDK/procfs
       quirk in this image), which caused a false "not running" report
       and let a second consumer instance start alongside a live one.
   (A plain `pgrep -f streaming_consumer.py` guard was tried before either
   of those — the wrapper script's own command-line text contains that
   same substring, so pgrep matched the wrapper itself and always
   reported "already running," even when no consumer was running at all.)

2. check_stream_table
   Runs check_streaming.py which counts rows in both stream tables.
   Fails (non-zero exit) if the streaming table is still empty after a
   producer run, indicating that the consumer is not writing to Iceberg.

3. build_gold_streaming_summary
   Runs gold_streaming_summary.py, which dedups the Bronze stream table
   into demo.silver.user_events_stream_clean and aggregates it into
   demo.gold.realtime_event_summary — the table the dashboard reads to
   show that streaming data actually reached the business layer.

Prerequisites
─────────────
• processing/docker-compose.yml services must be up (spark-master, minio, iceberg-rest)
• streaming/docker-compose.yml services must be up (kafka, kafka-producer)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import on_failure_alert

log = logging.getLogger(__name__)

SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

# Launches the streaming consumer as a detached background process inside the
# spark-master container. A PID file guards against starting a second
# instance (see the note above on why a pgrep-based guard doesn't work here).
START_CONSUMER_CMD = (
    "docker exec spark-master bash -c '"
    "PIDFILE=/tmp/streaming_consumer.pid; "
    "STATE=$( [ -f \"$PIDFILE\" ] && ps -o stat= -p \"$(cat \"$PIDFILE\")\" 2>/dev/null | tr -d \" \" ); "
    "if [ -n \"$STATE\" ] && [ \"${STATE:0:1}\" != \"Z\" ]; then "
    "  echo \"consumer already running (pid $(cat \\\"$PIDFILE\\\"), state $STATE)\"; "
    "else "
    "  nohup /opt/spark/bin/spark-submit "
    "  --master spark://spark-master:7077 "
    "  --conf spark.ui.enabled=false "
    "  /jobs/streaming_consumer.py "
    "  > /tmp/streaming_consumer.log 2>&1 & "
    "  echo $! > \"$PIDFILE\"; "
    "  disown; "
    "  echo \"consumer started (pid $(cat \\\"$PIDFILE\\\"))\"; "
    "fi'"
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "on_failure_callback": on_failure_alert,
    "email_on_failure": False,
}

with DAG(
    dag_id="streaming_pipeline",
    default_args=default_args,
    description="Ensure Kafka → Iceberg streaming consumer is running and verify data flow",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["streaming", "kafka", "lakehouse"],
) as dag:

    # ── 1. Start consumer if it is not already running ───────────────────────
    start_consumer = BashOperator(
        task_id="start_streaming_consumer",
        bash_command=START_CONSUMER_CMD,
        execution_timeout=timedelta(minutes=2),
    )

    # ── 2. Verify the stream table is receiving rows ─────────────────────────
    check_table = BashOperator(
        task_id="check_stream_table",
        bash_command=f"{SPARK_SUBMIT} /jobs/check_streaming.py",
        execution_timeout=timedelta(minutes=5),
    )

    # ── 3. Build the Silver-clean + Gold real-time summary from the stream ───
    build_streaming_gold = BashOperator(
        task_id="build_gold_streaming_summary",
        bash_command=f"{SPARK_SUBMIT} /jobs/gold_streaming_summary.py",
        execution_timeout=timedelta(minutes=10),
    )

    start_consumer >> check_table >> build_streaming_gold
