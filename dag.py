from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging

with DAG(
	'universidades_h',
	description='Configurar un DAG, sin consultas, ni procesamiento',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
	) as dag: