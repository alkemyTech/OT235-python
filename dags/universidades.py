from datetime import timedelta,timetime
from airflow import DAG
with DAG(
    'universidad',
    description='informacion de las universidades de 3 de febrero y utn'
    schedule_interval=timedelta(day=1),
    start_date=(2020,6,20),
) as dag:
    pass