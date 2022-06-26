from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging

def upload_to_s3(filename: str,  bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
	's3',
	description='subir_s3',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
) as dag:
    tarea_3=PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={'filename':'../files/u_cine.csv','key':'u_cine.csv','bucket_name':'cohorte-junio-a192d78b'}
    )
    tarea_4=PythonOperator(
        task_id='upload_to_s3_2',
        python_callable=upload_to_s3,
        op_kwargs={'filename':'../files/uba.csv','key':'uba.csv','bucket_name':'cohorte-junio-a192d78b'}
    )

    tarea_3 >> tarea_4