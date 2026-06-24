from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


default_args = {
    "owner": "saime",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="dataops_clean_transactions",
    default_args=default_args,
    start_date=datetime(2026, 6, 24),
    schedule=None,
    catchup=False,
    tags=["dataops", "rustfs", "postgres", "ssh"],
) as dag:

    run_cleaning_on_spark_client = SSHOperator(
        task_id="run_cleaning_on_spark_client",
        ssh_conn_id="spark_ssh",
        command="""
        set -e

        python -m pip install pandas boto3 sqlalchemy psycopg2-binary s3fs

        rm -rf /tmp/dataops-assignment

        git clone -b main https://github.com/saimeonver/dataops-assignment.git /tmp/dataops-assignment

        cd /tmp/dataops-assignment

        python dataops_app/clean_transactions.py
        """,
        cmd_timeout=300,
    )

    run_cleaning_on_spark_client