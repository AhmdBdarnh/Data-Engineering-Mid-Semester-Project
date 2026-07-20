"""
alerting.py
────────────
Shared Airflow failure-alert callback used by both batch_pipeline and
streaming_pipeline.

Behaviour
─────────
1. Always logs the failure (DAG id, task id, execution date, exception,
   log URL) — this happens unconditionally and never fails.
2. If ALERT_SMTP_HOST and ALERT_EMAIL_TO are both set, also attempts to
   send a plain-text email via Python's standard library smtplib. No
   external notification package is required.
3. If they are not set, logs a clear line stating that external alerting
   was skipped, so it's obvious from the logs whether alerting is
   configured.
4. Never raises — a failure while trying to send the alert must not mask
   or replace the original task failure.

Configuration (all optional; unset = alerting disabled, logging only):
    ALERT_EMAIL_TO        comma-separated recipient list, e.g. "a@x.com,b@x.com"
    ALERT_EMAIL_FROM       sender address (default: "airflow@lakehouse.local")
    ALERT_SMTP_HOST        SMTP server host
    ALERT_SMTP_PORT         SMTP server port (default: 587)
    ALERT_SMTP_USER        SMTP username (optional, for authenticated relays)
    ALERT_SMTP_PASSWORD    SMTP password (optional; NEVER hardcode this —
                            set it in the shell/.env before `docker compose up`)

To test: temporarily set the env vars above (e.g. a local Mailhog/smtp4dev
container or a real SMTP relay), restart the orchestration stack, then mark
any task as failed from the Airflow UI ("Clear" -> let it fail, or use
"Mark as failed") and check the task log for either
"alert email sent to ..." or "external alerting skipped".
"""

import logging
import os
import smtplib
from email.mime.text import MIMEText

log = logging.getLogger(__name__)


def _send_email(subject: str, body: str) -> bool:
    smtp_host = os.getenv("ALERT_SMTP_HOST", "")
    to_addrs = [a.strip() for a in os.getenv("ALERT_EMAIL_TO", "").split(",") if a.strip()]

    if not smtp_host or not to_addrs:
        log.info("Alerting: external alerting skipped (ALERT_SMTP_HOST / ALERT_EMAIL_TO not configured)")
        return False

    smtp_port = int(os.getenv("ALERT_SMTP_PORT", "587"))
    smtp_user = os.getenv("ALERT_SMTP_USER", "")
    smtp_password = os.getenv("ALERT_SMTP_PASSWORD", "")
    from_addr = os.getenv("ALERT_EMAIL_FROM", "airflow@lakehouse.local")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, to_addrs, msg.as_string())
        log.info("Alerting: alert email sent to %s", ", ".join(to_addrs))
        return True
    except Exception as exc:
        log.warning("Alerting: failed to send alert email (%s) — failure was still logged above", exc)
        return False


def on_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = getattr(task_instance, "log_url", "n/a") if task_instance else "n/a"

    log.error(
        "Task %s in DAG %s failed on %s | exception=%s | log_url=%s",
        task_id, dag_id, execution_date, exception, log_url,
    )

    subject = f"[Airflow] {dag_id}.{task_id} failed"
    body = (
        f"DAG:        {dag_id}\n"
        f"Task:       {task_id}\n"
        f"Executed:   {execution_date}\n"
        f"Exception:  {exception}\n"
        f"Log URL:    {log_url}\n"
    )
    _send_email(subject, body)
