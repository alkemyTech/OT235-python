from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
"""
configuration of the DAG without queries or processing for University of Palermo
"""
with DAG(
        'DAG_Universidad_de_Palermo',
        description='DAG para la Universidad de Palermo',
        # execute each one hour
        schedule_interval="@hourly", 
        start_date=datetime(2022, 6, 19)
        ) as dag:
            # Data extraction, transformation and loading tasks are declared
            extract_task= DummyOperator (task_id='extract_task')
            transform_task= DummyOperator (task_id='transform_task')
            load_task= DummyOperator (task_id='load_task')
            # the execution order of the DAG
            extract_task >> transform_task >> load_task