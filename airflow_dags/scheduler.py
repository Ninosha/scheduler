from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectUpdateSensor
from google.cloud import storage
import pandas as pd
from airflow.providers.google.common.utils import \
    id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession


def updated_file_name():
    PROJECT_name = "My First Project"
    BUCKET_name = "crudtask"

    client = storage.Client()
    bucket = client.bucket(BUCKET_name, PROJECT_name)

    file = bucket.blob("history.json")
    print(file)

    df = pd.read_json("gs://crudtask/history.json")

    file_name_list = [filename for filename in df
                      if df[filename]["status"] == "updated"]

    if file_name_list:
        Variable.set(key="file_names", value=file_name_list)


def invoke_cloud_function():
    url = "https://europe-west1-fair-solution-345912.cloudfunctions" \
          ".net/to_update_bucket"
    # the url is also the target audience.
    request = google.auth.transport.requests.Request()  # this is a
    # request for obtaining the the credentials
    id_token_credentials = id_token_credential_utils. \
        get_default_id_token_credentials(url, request=request)

    filenames = Variable.get("file_names")
    AuthorizedSession(id_token_credentials).request(
        method="POST",
        url=url,
        json={"filenames": filenames}
    )


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
    schedule_interval="* * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)

# t1 = GCSObjectUpdateSensor(
#     task_id='gcs_file_sensor_yesterday_task',
#     bucket='crudtask',
#     object="logs.csv",
#     ts_func = ,
#     dag=dag,
#
# )

t1 = PythonOperator(
    task_id='get_folders',
    python_callable=updated_file_name,
    dag=dag
)

t2 = PythonOperator(task_id="invoke_cf",
                    python_callable=invoke_cloud_function)

# t3 = CloudFunctionInvokeFunctionOperator(
#     function_id="ID of the function to be called str",
#     input_data="Input to be passed to the function dict",
#     location="The location where the function is located. str",
#     project_id='project_id str'
# )

t1 >> t2
