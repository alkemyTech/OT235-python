import logging
import boto3
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#   Configuracion del logger
logging.basicConfig(
    filename='main.log',
    level='DEBUG',  
    format="%(asctime)s:%(levelname)s:%(message)s",
)


"""Function to load data S3"""
#Upload file .txt to AWS S3 with boto3
def upload_to_s3():
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file('lat_sociales.txt', 'bucket', 'lat_sociales.txt')
        response = s3_client.upload_file('kennedy.txt', 'bucket', 'lat_sociales.txt')
    except ClientError as e:
        logging.error(e)
        return False
    return True


logging.info('Inicio del ETL para el grupo de universidades G.')

default_args={
    'owner' : 'lugones_nicolas',
    'retries' : 5,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval':"@hourly",
}


""" Definicion del DAG """
with DAG(
    'Operador de S3 Alkemy',
    default_args=default_args,
    description='Dag S3 Operator',
    start_date=datetime(2022, 6, 29),

) as dag:
    upload_s3=PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={"x" : "Apache Airflow"},
        dag=dag,
    )


    upload_s3


logging.info('Se corrio el DAG exitosamente.')
