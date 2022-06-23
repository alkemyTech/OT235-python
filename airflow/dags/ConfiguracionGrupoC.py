from datetime import datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    'DAG_Universidad',
    description='DAG para la Universidad',
    # execute each one hour
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    ) as dag:
        pass