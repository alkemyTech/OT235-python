import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


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
    extract = DummyOperator(task_id='extract')
    transform = DummyOperator(task_id='transform')
    load = DummyOperator(task_id='load')

    extract >> transform >> load