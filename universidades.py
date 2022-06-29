import logging
import psycopg2
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#   Configuracion del logger
logging.basicConfig(
    filename='main.log',
    level='DEBUG',  
    format="%(asctime)s:%(levelname)s:%(message)s",
)


"""Function to extract data to sql"""
#Extrac data from SQl host and export in csv files
def extract():
    try:
        connection=psycopg2.connect(
                host='training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com',
                user='alkymer2',
                password='Alkemy23',
                database='training'
        )

        cursor=connection.cursor()
        lat_sociales_open = open('lat_sociales_query.sql','r',encoding='utf-8')
        lat_sociales_query=lat_sociales_open.read()
        df=pd.read_sql(lat_sociales_query, connection)
        df.to_csv(f'lat_sociales_data.csv',index=False)

        kennedy_open = open('kennedy_query.sql','r',encoding='utf-8')
        kennedy_query=kennedy_open.read()
        df=pd.read_sql(kennedy_query, connection)
        df.to_csv(f'lat_sociales_data.csv',index=False)


    except Exception as e:
        print(e)

    finally:
        cursor.close()
        connection.close()
        print('Conexion finalizada')


"""Function to transform data"""
#Transform data whit pandas and export data in txt
def transform():
    def latino_transform():
        pass

    def kennedy_transform():
        pass

    latino_transform()
    kennedy_transform()


"""Function to load data S3"""
def load():
    pass


logging.info('Inicio del ETL para el grupo de universidades G.')

default_args={
    'owner' : 'lugones_nicolas',
    'retries' : 5,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval':"@hourly",
}


""" Definicion del DAG """
with DAG(
    'ETL_Universidades_Alkemy',
    default_args=default_args,
    description='Dag para las Universidades',
    start_date=datetime(2022, 6, 29),

) as dag:
    extract_sql=PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={"x" : "Apache Airflow"},
        dag=dag,
    )


    extract_sql 


logging.info('Se corrio el DAG exitosamente.')
