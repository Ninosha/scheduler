from google.cloud import storage
import os


def move_blob(request):
    """
    function is triggered when request is sent.
    moves updated file from source bucket to updated bucket.

    :param request: request sent from cloud composer
    :return:str/message
    """

    try:
        request_json = request.get_json()
        file_names_list = request_json["updated_files"]

    except ValueError as f:
        return ValueError(f)

    bucket_name = os.environ.get("bucket_name")
    destination_bucket_name = os.environ.get("destination_bucket_name")

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)

    destination_bucket = storage_client.bucket(destination_bucket_name)

    try:
        for blob_name in file_names_list:
            source_blob = source_bucket.blob(blob_name)

            destination_blob_name = blob_name

            source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name
            )

        return "file/files moved successfully"

    except FileNotFoundError as e:
        return FileNotFoundError(e)

    except NotImplementedError as e:
        return NotImplementedError(e)

