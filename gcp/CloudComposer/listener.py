from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from google.cloud import storage
from airflow.providers.google.common.utils import \
    id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def updated_file_name():
    """
    function listens to blob updates on a specific bucket/
    sets updated files list in airflow environment variables
    :return: str/message
    """
    BUCKET_name = Variable.get("bucket_name")

    try:

        client = storage.Client()
        bucket = client.get_bucket(BUCKET_name)

        blobs_list = bucket.list_blobs()
        updated_files = [blob for blob in blobs_list if blob.metadata
                         and blob.metadata["status"] == "updated"]

    except ConnectionError as f:
        return ConnectionError(f)

    except NotImplementedError as f:
        return NotImplementedError(f)

    blob_names = []
    for blob in updated_files:
        blob.metadata = {"status": "not updated"}
        blob.patch()
        blob_names.append(blob.name)

    if updated_files:
        Variable.set(key="file_names", value=blob_names)
        return "invoke_cf"
    else:
        return None


def invoke_cloud_function():
    """
    function triggers specific cloud function and passes filenames list
    to function via request

    :return: str/message
    """
    try:

        updated_files_list = eval(Variable.get("file_names"))

    except ValueError as e:
        return ValueError(e)

    if updated_files_list:

        Variable.set(key="updated_file_names", value=updated_files_list)

        url = "https://europe-west1-fair-solution-345912." \
              "cloudfunctions.net/to_update_bucket"
        try:
            request = google.auth.transport.requests.Request()
            id_token_credentials = id_token_credential_utils. \
                get_default_id_token_credentials(url, request=request)

            AuthorizedSession(id_token_credentials).request(
                method="POST",
                url=url,
                json={"updated_files": updated_files_list}
            )
            return "function triggered"

        except ConnectionError as f:
            return ConnectionError(f)

        except NotImplementedError as f:
            return NotImplementedError(f)

    else:
        return "files were not updated/function not triggered"


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

t1 = BranchPythonOperator(
    task_id='get_folders',
    python_callable=updated_file_name,
    dag=dag
)

t2 = PythonOperator(task_id="invoke_cf",
                    python_callable=invoke_cloud_function, dag=dag)

t3 = TriggerDagRunOperator(
    task_id="trigger_postgres_dag",
    trigger_dag_id="postgres_dag",
    reset_dag_run=False
)

t1 >> t2 >> t3
