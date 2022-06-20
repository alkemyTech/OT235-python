
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

### DAG with time interval between executions (1 hour) ###

with DAG(
    dag_id = 'universities_comahue_salvador',
    description = 'ETL dag for 2 universities',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022,6,20)    
) as dag:
    query_comahue = DummyOperator(task_id='query_comahue')
    query_salvador = DummyOperator(task_id='query_salvador')

    process_comahue = DummyOperator(task_id='process_comahue')
    process_salvador = DummyOperator(task_id='process_salvador')

    load_comahue = DummyOperator(task_id='load_comahue')
    load_salvador = DummyOperator(task_id='load_salvador')
        
    query_comahue >> process_comahue >> load_comahue
    query_salvador >> process_salvador >> load_salvador