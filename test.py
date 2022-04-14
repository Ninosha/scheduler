from CRUD.crud import CRUD

credentials = "/home/ninosha/Desktop/crud_task/Credentials/" \
              "fair-solution-345912-e1b814ef61f8.json"
project_name = "My First Project"
bucket_name = "crudtask"


obj = CRUD(credentials_url=credentials,
           project_name=project_name,
           bucket_name=bucket_name)

file_path_create = "/home/ninosha/Desktop/GCP_task_scheduler/Data/" \
                   "email-password-recovery-code.csv"

# obj.crud_obj.creat_file("as.csv", file_path_create)

obj.crud_obj.update_file("as.csv", file_path_create)
# obj.crud_obj.update_file("hvhj.csv", file_path_create)
#
# print(obj.crud_obj.read_file("hvhj.csv",
# "/home/ninosha/Desktop/GCP_task_scheduler/Downloads"))
# obj.crud_obj.delete_file("hvhj.csv")
