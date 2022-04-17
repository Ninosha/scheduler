from CRUD.crud_funcs import CRUD

credentials = "/home/ninosha/Desktop/crud_task/Credentials/" \
              "fair-solution-345912-e1b814ef61f8.json"
project_name = "My First Project"
bucket_name = "crudtask"


obj = CRUD(credentials_url=credentials,
           project_name=project_name,
           bucket_name=bucket_name)

file_path_create = "/home/ninosha/Downloads/5m-Sales-Records1.csv"

# create

# obj.crud_obj.create_file("5m-sales-records.csv", file_path_create)

# update

obj.crud_obj.update_file("5m-sales-records.csv", file_path_create)
# read

# path_to_download = "/home/ninosha/Desktop/GCP_task_scheduler
# /Downloads"

# obj.crud_obj.read_file("5m-sales-records.csv.csv", path_to_download)

# delete

# obj.crud_obj.delete_file("5m-sales-records.csv.csv")


