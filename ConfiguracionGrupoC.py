from datetime import datetime
from datetime import timedelta

from airflow import DAG

# configuration of retries
default_args = {
'owner': 'airflow',
'retries': 5,
'retry_delay': timedelta(minutes=5)
}

with DAG(
    'DAG_Universidad',
    description='DAG para la Universidad',
    default_args=default_args,
    # execute each one hour
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    ) as dag:
        pass