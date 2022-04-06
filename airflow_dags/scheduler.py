import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

file_name = ""

def if_updated(file_name):
    

    new_hash = blob(file_name).m5_hash

    try:
        old_hash = Variable.get(f"{file_name}_old")
    except Exception as f:
        old_hash = Variable.set(key=f"{file_name}_old", value=new_hash)

    if old_hash != new_hash:
        return file_name

bucket = "europe-central2-taxi-enviro-95a34f0f-bucket"

default_args = {
                   'depends_on_past': False,
                   'email': ['airflow@example.com'],
                   'email_on_failure': False,
                   'email_on_retry': False,
                   'retries': 1,
                   'retry_delay': timedelta(minutes=5),
               },
with DAG(
        'listener_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket="europe-central2-taxi-enviro-95a34f0f-bucket",
        source_object=file_name,
        destination_bucket="gcp_task_sweeft",
        destination_object=file_name,
    )

