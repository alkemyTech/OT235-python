from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# data normalization
def transform():
    pass

# configuration of logs
def my_logs():
    logging.basicConfig(format='%(asctime)s - %(name)s  - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG)
    logger= logging.getLogger(__name__)


# configuration of retries
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'DAG_UniversidadesC',
    description='DAG para la Universidad',
    default_args=default_args,
    # execute each one hour
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    ) as dag:
    # logging task
    logging_task = PythonOperator(task_id='logging_task', python_callable=my_logs)
    # normalization task
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform)
    logging_task >> transform_task