import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

#   Configuracion del logger
logging.basicConfig(
    filename='main.log',
    level='DEBUG',  
    format="%(asctime)s:%(levelname)s:%(message)s",
)

logging.info('Inicio del ETL para el grupo de universidades G.')

default_args={
    'owner' : 'lugones_nicolas',
    'retries' : 5,
}

""" Definicion del DAG """
with DAG(
    'universidades',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 6, 20),

) as dag:
    pass

logging.info('Se corrio el DAG exitosamente.')
