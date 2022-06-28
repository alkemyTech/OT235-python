from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

#  configuration of logs
def my_logs():
    logging.basicConfig(format='%(asctime)s - %(name)s  - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG)


logger = logging.getLogger(__name__)

def process_Jujuy():
    pass

def process_Palermo():
    pass


def db_process():
    process_Jujuy()
    process_Palermo()


# configuration of retries
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    'DAG_UniversidadesC',
    description='DAG para el grupo Universidades C',
    default_args=default_args,
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    
    ) as dag:
    # tarea de logging
    logging_task = PythonOperator(task_id='my_logs', python_callable=my_logs)
    process_task = PythonOperator(task_id='db_process', python_callable=db_process)
    logging_task >> process_task