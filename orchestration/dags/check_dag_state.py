"""
check_dag_state.py
────────────────────
Tiny helper invoked by start.sh / start.ps1 to poll a DAG run's state from
inside the airflow-webserver container, without needing fragile inline
Python-in-shell string quoting (a PowerShell here-string version of this
check previously broke Windows PowerShell 5.1's parser).

This file lives in orchestration/dags/ purely because that directory is
already volume-mounted into the Airflow containers
(./dags:/opt/airflow/dags in orchestration/docker-compose.yml) — it does
not define a DAG object, so Airflow's DAG file processor skips it (same
reasoning as dags/alerting.py, which is a plain helper module too).

Usage (run inside the airflow-webserver or airflow-scheduler container):
    python3 /opt/airflow/dags/check_dag_state.py <dag_id> <run_id>

Prints the DagRun's state ("success", "failed", "running", "queued", ...)
or "missing" if no such run exists.
"""

import sys

from airflow.models import DagRun


def main():
    if len(sys.argv) != 3:
        print("usage: check_dag_state.py <dag_id> <run_id>", file=sys.stderr)
        sys.exit(2)

    dag_id, run_id = sys.argv[1], sys.argv[2]
    runs = DagRun.find(dag_id=dag_id, run_id=run_id)
    print(runs[0].state if runs else "missing")


if __name__ == "__main__":
    main()
