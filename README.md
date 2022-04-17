# GCP Task Scheduler
 GCP Task Scheduler is a python module connected to GCP. 
Module contains CRUD, create, read, update and delete, functionality. 
Cloud Composer/Apache Airflow is a listener and is triggered when file 
content is updated and dag triggers a cloud function as a response.
Cloud Function moves updated file from original bucket to updated bucket,
listener dag also triggers dag to insert updated data into Postgres 
 database.


## Use

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install foobar
```

## Usage

```python
import foobar

# returns 'words'
foobar.pluralize('word')

# returns 'geese'
foobar.pluralize('goose')

# returns 'phenomenon'
foobar.singularize('phenomena')
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)