from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging



def get_data():

# Instantiate sqlachemy.create_engine object
    engine = create_engine('postgresql://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training')

  #Define SQL query
    with engine.connect() as con:
        with open("../universidades_h.sql") as file:
            query = file.read()
            result=query.split("#")

    #Read data into pandas dataframe
            df = pd.read_sql(result[0], con)

            u_cine_logger.info('Consulta Universidad Del Cine')

            df.to_csv('../files/u_cine.csv', index=False)

            df = pd.read_sql(result[1], con)
            
            df.to_csv('../files/uba.csv', index=False)
            
            uba_logger.info('Consulta UBA')


with DAG(
	'obtener_info',
	description='obtener_del_sql',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
) as dag:

	tarea_1=PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        retries= 5,
        retry_delay=timedelta(minutes=2),
        )


	tarea_1 