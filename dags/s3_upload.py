from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def upload_to_s3(filename: str,  bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
	's3_dag_uni_a',
	description='upload to s3 .txt files of the universities',
	schedule_interval='@hourly',
	start_date=datetime(2022, 7, 3),
    catchup=False
) as dag:
    """
    filename= directory of files to upload
    key= name and format with which the file is saved
    bucket_name= s3 bucket name
    """
    task_upload_to_s3_flores= PythonOperator(
        task_id= 'upload_to_s3_f',
        python_callable= upload_to_s3,
        op_kwargs={
            'filename': '/home/cris/airflow/dags/files/universidad_f_normalized.txt',
            'key': 'universidad_f_normalized.txt',
            'bucket_name': 'cohorte-junio-a192d78b'
            }
    )

    task_upload_to_s3_maria= PythonOperator(
        task_id= 'upload_to_s3_m',
        python_callable= upload_to_s3,
        op_kwargs={
            'filename': '/home/cris/airflow/dags/files/universidad_v_m_normalized.txt',
            'key': 'universidad_f_normalized.txt',
            'bucket_name': 'cohorte-junio-a192d78b'
            }
    )
    task_upload_to_s3_flores >> task_upload_to_s3_maria

