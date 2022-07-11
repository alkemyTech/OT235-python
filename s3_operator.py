import logging
import boto3
from botocore.exceptions import NoCredentialsError
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

ACCESS_KEY = 'AKIA24X5Z5Y2H4TL52MP'
SECRET_KEY = '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'

#Upload file .txt to AWS S3 with boto3
def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


uploaded = upload_to_aws('lat_sociales_data.txt', 'alkemy-acceleracion-lugonesnicolas', 'lat_sociales_data.txt')
uploaded = upload_to_aws('kennedy_data.txt', 'alkemy-acceleracion-lugonesnicolas', 'kennedy_data.txt')


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
