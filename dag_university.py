from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator

"""
DAG Description:
This DAG process information from universities
    - Universidad Nacional De Río Cuarto
    - Universidad De Morón
As for now only DummyOperator is used. Later PostgresOperator and PythonOperator will be needed.
"""


default_args = {
    'owner': 'guidocaru'
}

#DAG definition
with DAG(
    'universities_f',
    description='universities of group f',
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2022, 6, 22),
    catchup=False
) as dag:
    extract = DummyOperator(task_id='extract')
    transform = DummyOperator(task_id='transform')
    load = DummyOperator(task_id='load')

    extract >> transform >> load
