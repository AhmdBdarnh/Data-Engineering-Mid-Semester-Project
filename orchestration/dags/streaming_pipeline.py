"""
streaming_pipeline.py
──────────────────────
Hourly DAG that ensures the Kafka → Iceberg streaming consumer is alive
and that rows are landing in demo.bronze.user_events_stream.

Tasks
─────
1. start_streaming_consumer
   Launches streaming_consumer.py as a detached background process inside
   spark-master.  Idempotent: does nothing if the process is already running.

2. check_stream_table
   Runs check_streaming.py which counts rows in both stream tables.
   Fails (non-zero exit) if the streaming table is still empty after a
   producer run, indicating that the consumer is not writing to Iceberg.

Prerequisites
─────────────
• processing/docker-compose.yml services must be up (spark-master, minio, iceberg-rest)
• streaming/docker-compose.yml services must be up (kafka, kafka-producer)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

# Launches the streaming consumer as a detached background process inside the
# spark-master container.  pgrep prevents a second instance from starting.
START_CONSUMER_CMD = (
    "docker exec spark-master bash -c '"
    "if pgrep -f streaming_consumer.py > /dev/null 2>&1; then "
    "  echo \"streaming_consumer.py already running\"; "
    "else "
    "  nohup /opt/spark/bin/spark-submit "
    "  --master spark://spark-master:7077 "
    "  /jobs/streaming_consumer.py "
    "  > /tmp/streaming_consumer.log 2>&1 & "
    "  echo \"streaming_consumer.py started (PID $!)\"; "
    "fi'"
)


def _on_failure(context):
    log.error(
        "Task %s in DAG %s failed on %s",
        context["task_instance"].task_id,
        context["dag"].dag_id,
        context["execution_date"],
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "on_failure_callback": _on_failure,
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

    start_consumer >> check_table
