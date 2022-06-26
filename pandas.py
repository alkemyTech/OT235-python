from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging


def process_data():

    process_ucine()
    
    process_uba()

def process_ucine():
 
    df =pd.read_csv('../files/UBA_data.txt',sep=",")

    df['university']=df['university'].str.lower().str.strip().replace('-','')
    df['career']=df['career'].str.strip().str.lower().replace('-','')
    df['inscription_date']=pd.to_datetime(df['inscription_date']).dt.strftime('%Y-%m-%d')
    df2=df['full_name'].str.strip().str.lower().str.split('-',expand=True)
    df.drop(columns=['full_name'])
    df[['first_name','last_name']]=df2[[0,1]]
    df['gender']=df['gender'].str.replace('f','female').replace('m','male')
    df['age']=pd.to_numeric(df['age'])
    #df['postal_code']=df['postal_code'] No esta en el drive el archivo de codigos postales
    df['email']=df['email'].str.strip().str.lower().replace('-','')

    df.to_csv('../files/uba.csv', index=False)

def process_uba():

    df =pd.read_csv('../files/UBA_data.txt',sep=",")

    df['university']=df['university'].str.lower().str.strip().replace('-','')
    df['career']=df['career'].str.strip().str.lower().replace('-','')
    df['inscription_date']=pd.to_datetime(df['inscription_date']).dt.strftime('%Y-%m-%d')
    df2=df['full_name'].str.strip().str.lower().str.split('-',expand=True)
    df.drop(columns=['full_name'])
    df[['first_name','last_name']]=df2[[0,1]]
    df['gender']=df['gender'].str.replace('f','female').replace('m','male')
    df['age']=pd.to_numeric(df['age'])
    #df['postal_code']=df['postal_code'] No esta en el drive el archivo de codigos postales
    df['email']=df['email'].str.strip().str.lower().replace('-','')

    df.to_csv('../files/uba.csv', index=False)


with DAG(
	'universidades_h',
	description='Configurar un DAG, sin consultas, ni procesamiento',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
) as dag:

	tarea_2=PythonOperator(
        task_id='process_data',
        python_callable=process_data
        )
	

	tarea_2