from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3Hook import S3Hook
from datetime import datetime, timedelta
import logging
# import pandas as pd
from pathlib import Path
# import os
# import sys

#  configuration of logs
def my_logs():
    logging.basicConfig(format='%(asctime)s - %(name)s  - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG)


logger = logging.getLogger(__name__)
files_path = Path(__file__).parent.resolve()
filename = 'UBA.txt'

def load_s3():
    hook = S3Hook(aws_conn_id="my_conn")
    hook.load_file(
        filename = f'/{files_path}/output/{filename}.txt',
        key = f'{filename}.txt',
        bucket_name = "cohorte-junio-a192d78b",
        replace=True,
    )

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
    'DAG_UniversidadesCC',
    description='DAG para el grupo Universidades C',
    default_args=default_args,
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    
    ) as dag:
    # tarea de logging
    logging_task = PythonOperator(task_id='my_logs', python_callable=my_logs)
    process_task = PythonOperator(task_id='db_process', python_callable=db_process)
    # tarea de normalizacion
    #transform_task = PythonOperator(task_id='normalize_data', python_callable=normalize_data)
    load_task = PythonOperator(task_id='load_s3', python_callable=load_s3)
    logging_task >> process_task  >> load_task