from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlalchemy


def insert_postgres():
    """
    function inserts updated file to postgres database
    :return: str/message
    """
    try:
        now = datetime.now()
        time = now.strftime("%m/%d/%Y, %H:%M:%S")

        engine = sqlalchemy.create_engine(
            'postgresql://postgres:nino1@34.79.23.170:5432/scheduler')

        updated_files_list = eval(Variable.get("updated_file_names"))

        for filename in updated_files_list:
            df = pd.read_csv(f'gs://updated_bucket/{filename}')
            df.to_sql(f"{filename}-{time}", engine)

        Variable.set(key="updated_file_names", value=[])

        return "updated file was inserted to postgres"

    except ConnectionError as f:
        return ConnectionError(f)

    except NotImplementedError as f:
        return NotImplementedError(f)


with DAG(
        'postgres_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0
        },
        description='A simple tutorial DAG',
        schedule_interval="@once",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    PythonOperator(task_id="to_postgres",
                   python_callable=insert_postgres)
