from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging


# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')

# add formatter to ch
ch.setFormatter(formatter)

u_cine_logger = logging.getLogger('u_cine_logger')
u_cine_logger.setLevel(logging.DEBUG)

uba_logger = logging.getLogger('uba_logger')
uba_logger.setLevel(logging.DEBUG)

# add ch to logger
u_cine_logger.addHandler(ch)
uba_logger.addHandler(ch)



filename = 'pivoted_data'
S3_CONN_ID = 'training'
BUCKET = 'training-bucket'

def get_data():
    #Make connection to DB
    hook = S3Hook('S3_CONN_ID')
    conn = hook.get_conn()

    #Define SQL query
    with open("airflow/universidades.sql") as file:
        query = text(file.read())
        result=query.split("#")

    #Read data into pandas dataframe
        df = pd.read_sql(result[0], conn)

        u_cine_logger.info('Consulta Universidad Del Cine')

    #Read data into pandas dataframe
        df = pd.read_sql(result[1], conn)

        uba_logger.info('Consulta UBA')


def pivot_data(**kwargs):
    #Pivot dataframe into new format
    pivot_df = df.pivot(index='DATE', columns='STATE', values='POSITIVE').reset_index()


def upload_to_s3(filename: str,  bucket_name: str) -> None:
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_hook.load_string(pivot_df.to_csv(index=False),
                        '{0}.csv'.format(filename),
                        bucket_name=BUCKET,
                        replace=True)

with DAG(
	'universidades_h',
	description='Configurar un DAG, sin consultas, ni procesamiento',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
	) as dag:

	
	tarea_1=PythonOperator(
            task_id='get_data',
            python_callable=get_data,
            retries= 5,
            retry_delay=timedelta(minutes=2),
        )

	tarea_2=PythonOperator(
            task_id='pivot_data',
            python_callable=pivot_data
        )

	tarea_3=PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3
        )

	tarea_1 >> tarea_2 >> tarea_3