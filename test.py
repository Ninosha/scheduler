from CRUD.crud import CRUD
from CRUD.utilities.utils import get_credentials
from gcloud import storage

credentials = "/home/ninosha/Desktop/crud_task/Credentials/" \
              "fair-solution-345912-e1b814ef61f8.json"
project_name = "My First Project"
bucket_name = "crudtask"


obj = CRUD(credentials_url=credentials,
           project_name=project_name,
           bucket_name=bucket_name)

file_path_create = "/home/ninosha/Desktop/GCP_task_scheduler/Data/" \
                   "email-password-recovery-code.csv"

# obj.crud_obj.creat_file("hvhj.csv", file_path_create)

obj.crud_obj.update_file("h.csv",
                         file_path_create)
#
# print(obj.crud_obj.read_file("hvhj.csv",
# "/home/ninosha/Desktop/GCP_task_scheduler/Downloads"))
# obj.crud_obj.delete_file("hvhj.csv")

client = storage.Client(
    credentials=get_credentials(credentials),
    project=project_name
)


# bucket = client.get_bucket(bucket_name)
# blobs = bucket.list_blobs()
# for b in blobs:
#     metadata = {
#         "Blob_Name": b.name,
#         "Size": b.size,
#         "Updated": b.updated
#     }
#     print(metadata)


#
# from google.cloud import storage
#
# def hello_gcs(event, context):
#      storage_client = storage.Client()
#      base_bucket = storage_client.get_bucket("crudtask")
#      logs = base_bucket.blob("logs.csv")
#      file = event
#      print(f"Processing file: {logs['name']}.")




#
# from google.cloud import storage
# import pandas as pd
#
#
#
# def hello_gcs(event, context):
#     file_name = "logs.csv"
#     df = pd.read_csv(f"gs://updated_logs/{file_name}")
#     last_row = df.[status].iloc[-1]

#     print(f"Processing file: {df}.")
#


