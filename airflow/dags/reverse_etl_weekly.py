"""
Reverse ETL DAG for Customer Weekly Metrics
Extracts weekly customer metrics and syncs to Salesforce
Owner: terrabog
"""
import json
import logging
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

PROJECT_DIR = "/opt/project"
BASE_CMD = f"cd {PROJECT_DIR} && python -m extractor.cli --use-checkpoint --log-format json"


# ---------------------------------------------------------------------------
# Failure alerting callback
# ---------------------------------------------------------------------------

def on_task_failure(context):
    """Callback fired when any task in the DAG fails. Logs details and sends Slack notification."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]
    log_url = task_instance.log_url
    exception = context.get("exception", "Unknown")

    message = (
        f"ALERT: Task `{task_id}` failed in DAG `{dag_id}`\n"
        f"Execution date: {execution_date}\n"
        f"Exception: {exception}\n"
        f"Log: {log_url}"
    )

    logger.error(message)

    # Attempt Slack notification if webhook is configured
    slack_url = Variable.get("slack_webhook_url", default_var="")
    if slack_url:
        try:
            payload = json.dumps({"text": message}).encode("utf-8")
            req = Request(slack_url, data=payload, headers={"Content-Type": "application/json"})
            urlopen(req, timeout=10)
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")


# ---------------------------------------------------------------------------
# DAG default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "terrabog",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
    "on_failure_callback": on_task_failure,
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="reverse_etl_customer_weekly",
    description="Reverse ETL weekly customer metrics extraction and Salesforce sync",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule="0 14 * * 1-5",  # Mon-Fri at 2pm UTC
    catchup=False,
    tags=["reverse-etl", "salesforce", "weekly"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"{BASE_CMD} --stage extract",
        env={"PYTHONUNBUFFERED": "1"},
        execution_timeout=timedelta(hours=2),
    )

    validate_and_transform = BashOperator(
        task_id="validate_and_transform",
        bash_command=f"{BASE_CMD} --stage transform",
        env={"PYTHONUNBUFFERED": "1"},
        execution_timeout=timedelta(minutes=30),
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"{BASE_CMD} --stage load",
        env={"PYTHONUNBUFFERED": "1"},
        execution_timeout=timedelta(minutes=30),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"{BASE_CMD} --stage quality-gate",
        env={"PYTHONUNBUFFERED": "1"},
        execution_timeout=timedelta(minutes=5),
    )

    salesforce_sync = BashOperator(
        task_id="salesforce_sync",
        bash_command=f"{BASE_CMD} --stage salesforce-sync",
        env={
            "PYTHONUNBUFFERED": "1",
            "SF_USERNAME": "{{ var.value.sf_username }}",
            "SF_PASSWORD": "{{ var.value.sf_password }}",
            "SF_SECURITY_TOKEN": "{{ var.value.sf_security_token }}",
            "SF_DOMAIN": "{{ var.value.get('sf_domain', 'login') }}",
        },
        execution_timeout=timedelta(minutes=30),
    )

    extract >> validate_and_transform >> load >> quality_gate >> salesforce_sync
