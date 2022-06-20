from datetime import datetime

from datetime import timedelta

from airflow import DAG

from airflow.operators.dummy import DummyOperator

"""
Configure the retries with the connection to the database for the National University of Jujuy
"""

#configuration of retries
Default_args = {
'owner': 'airflow',
'retries': 5,
'retry_delay': timedelta(minutes=5)
}

with DAG(
        'DAG_Universidad_Nacional_de_Jujuy',
        description='DAG para la Universidad Nacional de Jujuy',
        # execute each one hour
        schedule_interval="@hourly", 
        start_date=datetime(2022, 6, 19)
        ) as dag:
        # Data extraction, transformation and loading tasks are declared
            extract_task = DummyOperator(task_id='extract_task')
            transform_task = DummyOperator(task_id='transform_task')
            load_task = DummyOperator(task_id='load_task')
        # The execution order of the DAG
            extract_task >> transform_task >> load_task