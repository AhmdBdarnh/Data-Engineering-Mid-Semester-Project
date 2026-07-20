"""
batch_pipeline.py
──────────────────
Daily Bronze → Silver → Gold → Data-Quality ETL pipeline.

Each BashOperator task runs spark-submit inside the spark-master container
via "docker exec".  The processing stack (processing/docker-compose.yml)
must be running before this DAG is triggered.

Schedule: daily at midnight UTC.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import on_failure_alert

log = logging.getLogger(__name__)

# spark-submit prefix — runs the job on the standalone cluster inside the
# spark-master container (all Iceberg / S3A JARs and spark-defaults.conf
# are already baked into that container's image)
SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
    "email_on_failure": False,
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Daily Bronze → Silver → Gold batch ETL with data quality checks",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["batch", "etl", "lakehouse"],
) as dag:

    # ── 1. Ingest all CSVs into Bronze Iceberg tables ────────────────────────
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"{SPARK_SUBMIT} /jobs/bronze_ingestion.py",
        execution_timeout=timedelta(minutes=30),
    )

    # ── 2. Build Silver dimension tables (dim_product, dim_pricing SCD-2) ───
    silver_dimensions = BashOperator(
        task_id="silver_dimensions",
        bash_command=f"{SPARK_SUBMIT} /jobs/silver_dimensions.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ── 3. Build Gold fact + aggregate tables ────────────────────────────────
    gold_facts = BashOperator(
        task_id="gold_facts",
        bash_command=f"{SPARK_SUBMIT} /jobs/gold_facts.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ── 4. Run data-quality checks across all layers ─────────────────────────
    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=f"{SPARK_SUBMIT} /jobs/data_quality.py",
        execution_timeout=timedelta(minutes=15),
    )

    # ── 5. Bonus: Great Expectations-style validation report ─────────────────
    great_expectations = BashOperator(
        task_id="great_expectations_validation",
        bash_command=f"{SPARK_SUBMIT} /jobs/great_expectations_validation.py",
        execution_timeout=timedelta(minutes=15),
    )

    # ── 6. Bonus: DataHub lineage artifact / optional GMS emission ───────────
    datahub_lineage = BashOperator(
        task_id="emit_datahub_lineage",
        bash_command=f"{SPARK_SUBMIT} /jobs/emit_datahub_lineage.py",
        execution_timeout=timedelta(minutes=15),
    )

    # ── 7. Build HTML business dashboard from Gold layer ─────────────────────
    build_dashboard = BashOperator(
        task_id="build_dashboard",
        bash_command=f"{SPARK_SUBMIT} /jobs/build_dashboard.py",
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task dependencies ────────────────────────────────────────────────────
    bronze_ingestion >> silver_dimensions >> gold_facts >> data_quality
    data_quality >> great_expectations >> build_dashboard
    data_quality >> datahub_lineage >> build_dashboard
