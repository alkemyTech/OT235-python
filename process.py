from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging


def process_data():

    process_ucine()
    
    process_uba()

def process_ucine():
 
    pass

def process_uba():

    pass

with DAG(
	'process',
	description='procesar_la_info',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
	) as dag:

	tarea_2=PythonOperator(
        task_id='process_data',
        python_callable=process_data
        )
	tarea_2 