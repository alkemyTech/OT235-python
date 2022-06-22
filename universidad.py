import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


logging.basicConfig(
    level=logging.INFO, 
    datefmt='%Y-%m-%d', 
    format='%(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger('Universidades_G')

logger.info('ETL DAG para el grupo de universidades G')


with DAG(
    'universidades',
    owner= 'lugones_nicolas'
    description='ETL para Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval=timedelta(hours=1),
    retries=5,
    retry_delay= timedelta(minutes=5),
    start_date=datetime(2022, 6, 20),

) as dag:
    extract = DummyOperator(task_id='extract')
    transform = DummyOperator(task_id='transform')
    load = DummyOperator(task_id='load')

    extract >> load >> load