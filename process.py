from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging


def procesar_universidades_h_pandas():

    process_ucine()
    
    process_uba()

def process_ucine():
 
    pass

def process_uba():

    pass

with DAG(
	'procesar_universidades_h_pandas',
	description='procesar_universidades_h_pandas',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
	) as dag:

	tarea_1=PythonOperator(
        task_id='procesar_universidades_h_pandas',
        python_callable=procesar_universidades_h_pandas
        )
	
	tarea_1