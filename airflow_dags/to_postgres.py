from airflow import DAG

from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine


def insert_postgres():
    engine = create_engine(
        'postgresql://fair-solution-345912:europe-west1:schedulersql:'
        '5432/scheduler_database')

    updated_files_list = eval(Variable.get("updated_file_names"))

    for filename in updated_files_list:
        df = pd.read_csv(f'gs://updated_bucket/{filename}')
        df.to_sql(f"{filename}", engine)


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

cloud_storage_bucket_name = 'updated_bucket'

t1 = PythonOperator(task_id="to_postgres",
                    python_callable=insert_postgres)

t1
