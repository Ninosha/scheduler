from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import storage
from airflow.providers.google.common.utils import \
    id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession


def updated_file_name():

    BUCKET_name = "crudtask"

    client = storage.Client()
    bucket = client.get_bucket(BUCKET_name)
    blobs_list = bucket.list_blobs()
    updated_files = [blob for blob in blobs_list if blob.metadata
                     and blob.metadata["status"] == "updated"]

    blob_names = []
    for blob in updated_files:
        blob.metadata = {"status": "not updated"}
        blob.patch()
        blob_names.append(blob.name)

    Variable.set(key="file_names", value=blob_names)


def invoke_cloud_function():

    updated_files_list = eval(Variable.get("file_names"))

    if updated_files_list:
        url = "https://europe-west1-fair-solution-345912.cloudfunctions" \
              ".net/to_update_bucket"
        # the url is also the target audience.
        request = google.auth.transport.requests.Request()  # this is a
        # request for obtaining the the credentials
        id_token_credentials = id_token_credential_utils. \
            get_default_id_token_credentials(url, request=request)

        AuthorizedSession(id_token_credentials).request(
            method="POST",
            url=url,
            json={"updated_files": updated_files_list}
        )
    else:
        print("files were not updated")


dag = DAG(
    'listener_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0
    },
    description='A simple tutorial DAG',
    schedule_interval="* * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)

t1 = PythonOperator(
    task_id='get_folders',
    python_callable=updated_file_name,
    dag=dag
)
#
t2 = PythonOperator(task_id="invoke_cf",
                    python_callable=invoke_cloud_function)


# t3 = CloudFunctionInvokeFunctionOperator(
#     function_id="ID of the function to be called str",
#     input_data="Input to be passed to the function dict",
#     location="The location where the function is located. str",
#     project_id='project_id str'
# )

t1 >> t2
