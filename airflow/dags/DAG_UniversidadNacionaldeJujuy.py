from asyncio.format_helpers import extract_stack
import operator
from airflow import DAG 
from datetime import datetime,timedelta
from airflow.operators.dummy  import DummyOperator

"""
configuration of the DAG without queries or processing
for National University of Jujuy
"""
with DAG (
          'DAG_Universidad_Nacional_de_Jujuy',
          description='DAG para la Universidad Nacional de Jujuy',
          scheduler_interval=timedelta(hours=1), #execute each one hour
          start_date=datetime(2022,6,19)
          ) as dag:
            #only the tasks of extrancting data , transforming them and uploading them are declare  
                extract_task=DummyOperator(task_id='extract_task')
                transform_task=DummyOperator(task_id='transform_task')
                load_task=DummyOperator(task_id='transform_task')
            #the execution order of the DAG
                extract_task >> transform_task >> load_task