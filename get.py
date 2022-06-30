from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging




def obtener_del_sql():

# Creo la conexion
    engine = create_engine('postgresql://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training')

  #Genero la conexion, abro el archivo de la query sql y la guardo en query_universidad
    with engine.connect() as con:
        with open("../universidades_h.sql") as file:
            query = file.read()
            query_universidad=query.split("#")

    #Leo del sql y guardo en dataframe pandas
            df_u_cine = pd.read_sql(query_universidad[0], con)

    #Guardo en files en csv
            df_u_cine.to_csv('../files/u_cine_universidades_h.csv', index=False)

    #Leo del sql y guardo en dataframe pandas
            df_uba = pd.read_sql(query_universidad[1], con)
    
    #Guardo en files en csv
            df_uba.to_csv('../files/uba_universidades_h.csv', index=False)
            


with DAG(
    'obtener_del_sql',
    description='obtener_del_sql',
    schedule_interval='@hourly',
    start_date=datetime(2022, 6, 21),
) as dag:

    tarea_1=PythonOperator(
        task_id='obtener_del_sql',
        python_callable=obtener_del_sql,
        retries= 5,
        retry_delay=timedelta(minutes=2),
        )


    tarea_1 
    