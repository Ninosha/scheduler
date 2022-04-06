from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor
from airflow.providers.google.cloud.operators.functions import \
    CloudFunctionInvokeFunctionOperator
from google.cloud import storage
import pandas as pd


def updated_file_name():
    PROJECT_name = "My First Project"
    BUCKET_name = "crudtask"
    client = storage.Client()
    bucket = client.bucket(BUCKET_name, PROJECT_name)
    file = bucket.blob("logs.csv")
    df = pd.read_csv(file)
    file_name = df["file"].iloc[-1]
    Variable.set(key="file_name", value=file_name)


default_args = {
                   'depends_on_past': False,
                   'email': ['airflow@example.com'],
                   'email_on_failure': False,
                   'email_on_retry': False,
                   'retries': 1,
                   'retry_delay': timedelta(minutes=5),
               },
dag = DAG(
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
)

t1 = GCSObjectUpdateSensor(
    task_id='gcs_file_sensor_yesterday_task',
    bucket='myBucketName',
    object="logs.csv"
)

t2 = PythonOperator(
    task_id='get_folders',
    python_callable=updated_file_name,
    dag=dag
)

# t3 = CloudFunctionInvokeFunctionOperator(
#     function_id="ID of the function to be called str",
#     input_data="Input to be passed to the function dict",
#     location="The location where the function is located. str",
#     project_id='project_id str'
# )

t1 >> t2
