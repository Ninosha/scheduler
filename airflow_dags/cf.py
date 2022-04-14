from typing import Any

import composer2_airflow_rest_api


def trigger_dag_gcf(data, context):
    """
    Trigger a DAG and pass event data.

    Args:
      data: A dictionary containing the data for the event. Its format depends
      on the event.
      context: The context object for the event.

    For more information about the arguments, see:
    https://cloud.google.com/functions/docs/writing/background#function_parameters
    """

    # TODO(developer): replace with your values
    # Replace web_server_url with the Airflow web server address. To obtain this
    # URL, run the following command for your environment:
    # gcloud
    # composer
    # environments
    # describe
    # example - environment - -location = us - central1 - -format = "value(config.airflowUri)"

    web_server_url = (
        "https://example-airflow-ui-url-dot-us-central1.composer.googleusercontent.com"
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = 'listener dag'
    data = {"file_name": data.filename, "bucket": data.bucket}

    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)


# oogle - auth == 2.5
# .0
# requests == 2.27
# .1'

from google.cloud import storage
from datetime import datetime
import os

now = datetime.now()

current_time = now.strftime("%H:%M:%S")

from google.cloud import storage
from datetime import datetime

now = datetime.now()

current_time = now.strftime("%H:%M:%S")


def move_blob(request):
    request_json = request.get_json()
    file_names_list = request_json["updated_files"]

    """Copies a blob from one bucket to another with a new name."""
    bucket_name = os.environ.get("bucket_name")
    # blob_name = "your-object-name"
    destination_bucket_name = os.environ.get("destination_bucket_name")
    # destination_blob_name = "destination-object-name"

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)

    destination_bucket = storage_client.bucket(destination_bucket_name)

    for blob_name in file_names_list:

        source_blob = source_bucket.blob(blob_name)

        destination_blob_name = f"updated_{blob_name}_{current_time}"

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )


google - cloud - storage
fsspec
gcsfs
