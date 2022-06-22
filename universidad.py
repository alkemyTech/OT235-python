import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

#   Configuracion del logger
logging.basicConfig(
    filename='main.log'
    level=logging.INFO,  
    format='%(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger('Universidades_G')

logger.info('ETL DAG para el grupo de universidades G')

""" Definicion del DAG """
with DAG(
    'universidades',
    owner= 'lugones_nicolas'
    description='ETL para Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval=timedelta(hours=1),
    retries=5,
    retry_delay= timedelta(minutes=5),
    start_date=datetime(2022, 6, 20),

) as dag:
    extract_u1 = DummyOperator(task_id='extract_u1')
    transform_u1 = DummyOperator(task_id='transform_u1')
    load_u1 = DummyOperator(task_id='load_u1')

    extract_u2 = DummyOperator(task_id='extract_u2')
    transform_u2 = DummyOperator(task_id='transform_u2')
    load_u2 = DummyOperator(task_id='load_u2')

#   Tareas a realizar
    extract_u1 >> load_u1 >> load_u1
    logger.info('Se corrio el ETL Facultad Latinoamericana De Ciencias Sociales')

    logger.info('Se corrio el ETL Universidad J. F. Kennedy')
    extract_u2 >> load_u2 >> load_u2