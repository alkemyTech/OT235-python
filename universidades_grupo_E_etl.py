"""\
# DAG for ETL process for two universities (group-E)

        - Universidad Nacional De La Pampa
        - Universidad Abierta Interamericana

## Operators needed:

        - PostgresOperator: for SQL code excecution
        - PythonOperator: for processing data and loading in S3
                        (S3Hook Class may be required)
"""
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from scripts.process_data import process_data_univ

# logging configuration
logging.basicConfig(level=logging.INFO,
                    datefmt='%Y-%m-%d',
                    format='%(asctime)s - %(name)s - %(message)s'
                    )

# Logger creation
logger = logging.getLogger('Universidades-E')

default_args = {
    'owner': 'pablo_correa',
}

with DAG(
        'universidades_grupo_E_etl',
        description='ETL DAG for two universities',
        default_args=default_args,
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 6, 21),
        catchup=False
) as dag:
    # log message with logger name at the beggining of DAG
    logger.info("")

    dag.doc_md = __doc__  # refers to docstring at the beginning of the DAG

    # tasks (dummy tasks must be replaced in future versions):
    # Retry configuration in Extract task where database connection takes place
    extract = DummyOperator(task_id='extract', retries=5,
                            retry_delay=timedelta(minutes=5))
    transform = PythonOperator(
        task_id='transform', python_callable=process_data_univ)
    load = DummyOperator(task_id='load')

    extract >> transform >> load
