import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from e_t_l import extract_data, transform_data, load_data


### LOGS ###

FORMAT = '%(asctime)s %(name)s %(message)s' # Message format for the log

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%m-%d',
    format=FORMAT)
logger = logging.getLogger('Universidades_B') # Logger's name
logger.info('ETL para UNCOM y USAL') # Logger's message


# Retries for connection

args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


### DAG with time interval between executions (1 hour) ###

with DAG(
        dag_id='universities_comahue_salvador',
        description='ETL dag for 2 universities',
        default_args=args,
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 6, 20)
) as dag:
    extract = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load = PythonOperator(task_id='transform_data', python_callable=load_data)

    extract >> transform >> load