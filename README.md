# GCP Task Scheduler
 GCP Task Scheduler is a python module that is used to modify files in 
 bucket and save to updated bucket to cloud storage and Postgres database. 
- Module contains CRUD: create, read, update and delete functionalities. 
- Cloud Composer/Apache Airflow is a listener and is triggered when file 
content is updated and dag triggers a cloud function as a response.
- Cloud Function moves updated file from original bucket to updated bucket.
- Listener dag also triggers dag to insert updated data into Postgres 
 database, with a table name "filename + time".


## Usage

Import CRUD module to use crud functionality

```python
from CRUD.crud_funcs import CRUD
```
To use CRUD functions, user needs to create crud object first

```python
# credentials url to authorise your google cloud client
credentials = "../user_credentials_url.json"
# project name
project_name = "Project Name"
# bucket name
bucket_name = "BucketName"

# create object with parameters needed 
obj= CRUD(
         credentials_url=credentials,
         project_name=project_name,
         bucket_name=bucket_name
)
```
To create file:

```python
# define url where the file user needs to upload from
file_path_to_upload = "../Data/example.csv"

# call the create_file function, pass the name to be saved in the bucket
# and file url already defined
obj.crud_obj.create_file("example_name.csv", file_path_to_upload)
```

To read a file function needs to download needed file:

```python
# define url where the file user needs to download file
file_path_to_download = "../Read_data"

# call update_file function, pass the name of  a file needed to be read
# and file url already defined
obj.crud_obj.read_file("example.csv",file_path_to_download)

```

To update a file:
```python
# define url where the file user needs to upload from
file_path_to_update = "../Data/example_update.csv"

# call update_file function, pass the name of  a file needed to be updated
# and file url already defined

obj.crud_obj.update_file("example_name.csv", file_path_to_update)
```
- when file is being updated program checks if file's content has changed, 
if it has Cloud Composer triggers Cloud Function which copies updated 
file from original bucket to updated bucket and triggers Second dag in 
composer to save file in postgres database.


 To delete a file:
```python
# call a function delete_file and pass the name of a file needed to be deleted
obj.crud_obj.delete_file("example.csv")
```

