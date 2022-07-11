from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging

#Genero el hook para subir el archivo al bucket
def subir_al_s3_universidades_h(filename: str,  bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
	'subir_al_s3_universidades_h',
	description='subir_al_s3_universidades_h',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
) as dag:
    tarea_1=PythonOperator(
        task_id='subir_al_s3_universidades_h',
        python_callable=subir_al_s3_universidades_h,
        #los parametros opcionales definen que archivo y con que nombre lo subo
        op_kwargs={'filename':'../files/u_cine_universidades_h.csv','key':'u_cine_universidades_h.csv','bucket_name':'cohorte-junio-a192d78b'}
    )
    tarea_2=PythonOperator(
        task_id='subir_al_s3_universidades_h_2',
        python_callable=subir_al_s3_universidades_h,
        #los parametros opcionales definen que archivo y con que nombre lo subo
        op_kwargs={'filename':'../files/uba_universidades_h.csv','key':'uba_universidades_h.csv','bucket_name':'cohorte-junio-a192d78b'}
    )

    tarea_1 >> tarea_2