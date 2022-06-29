from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging


def procesar_universidades_h_pandas():

    process_ucine()
    
    process_uba()

def process_ucine():
 
    df =pd.read_csv('../files/u_cine_universidades_h',sep=",")
    codigos_postales =pd.read_csv('../files/codigos_postales.csv',sep=",")

    df['universities']=df['universities'].str.lower().str.strip().replace('-','')
    df['careers']=df['careers'].str.strip().str.lower().replace('-','')
    df['inscription_dates']=pd.to_datetime(df['inscription_dates'], format='%d-%m-%Y', infer_datetime_format=True)
    reemplazar_caracteres=['MISS-','MS.-','MR.-','-DVM','-DDS','MRS.-','-MD','DR.-','-md','-dds','-JR.','-V','-II','-PHD','-IV','-III']
    for a in reemplazar_caracteres:
        df['names']=df['names'].str.replace(a,'')
    df2=df['names'].str.strip().str.lower().str.split('-',expand=True)
    df[['first_name','last_name']]=df2[[0,1]]
    df['sexo']=df['sexo'].str.replace('F','female').replace('M','male')
    df['birth_dates']=pd.to_datetime(df['birth_dates'], format='%d-%m-%Y', infer_datetime_format=True)
    df['age']=df['birth_dates'].map(lambda x : abs(relativedelta(date.today(),x)).years)
    df['locations']=df['locations'].str.replace('-',' ').str.strip()
    df3=pd.merge(df['locations'],codigos_postales,how='left',left_on=['locations'],right_on=['localidad'])
    df['postal_code']=df3['codigo_postal']
    df['locations']=df['locations'].str.lower()
    df['emails']=df['emails'].str.strip().str.lower().replace('-','')
    df.drop(columns=['birth_dates','names'],inplace=True)

    df.to_csv('../files/u_cine_universidades_h.txt', index=False)
    


def process_uba():

    df =pd.read_csv('../files/uba_universidades_h.csv',sep=",")
    codigos_postales =pd.read_csv('../files/codigos_postales.csv',sep=",")

    df['universidades']=df['universidades'].str.lower().str.strip().replace('-','')
    df['carreras']=df['carreras'].str.strip().str.lower().replace('-','')
    df['fechas_de_inscripcion']=pd.to_datetime(df['fechas_de_inscripcion'], format='%d-%b-%Y', infer_datetime_format=True)
    reemplazar_caracteres=['MISS-','MS.-','MR.-','-DVM','-DDS','MRS.-','-MD','DR.-','-md','-dds','-JR.','-V','-II','-PHD','-IV','-III']
    for a in reemplazar_caracteres:
        df['nombres']=df['nombres'].str.replace(a,'')
    df2=df['nombres'].str.strip().str.lower().str.split('-',expand=True)
    df[['first_name','last_name']]=df2[[0,1]]
    df['sexo']=df['sexo'].str.replace('F','female').replace('M','male')
    df['fechas_nacimiento']=df['fechas_nacimiento'].str.replace('00-','01-').replace('29-Feb-07','28-Feb-07')\
    .replace('31-Sep-20','30-Sep-20').replace('31-Apr-16','30-Apr-16').replace('31-Feb-14','28-Feb-14')\
    .replace('29-Feb-06','28-Feb-06').replace('31-Sep-09','30-Sep-09').replace('30-Feb-16','29-Feb-16')\
    .replace('31-Apr-04','30-Apr-04').replace('31-Sep-02','30-Sep-02').replace('31-Nov-09','30-Nov-09')\
    .replace('31-Nov-29','30-Nov-29').replace('30-Feb-18','28-Feb-18').replace('31-Jun-15','30-Jun-15')\
    .replace('31-Apr-10','30-Apr-10').replace('31-Sep-07','30-Sep-07')
    df['fechas_nacimiento']=pd.to_datetime(df['fechas_nacimiento'], format='%d-%b-%Y', infer_datetime_format=True)
    df['age']=df['fechas_nacimiento'].map(lambda x : abs(relativedelta(date.today(),x)).years)
    df3=pd.merge(df['codigos_postales'],codigos_postales,how='left',left_on=['codigos_postales'],right_on=['codigo_postal'])
    df['locations']=df3['localidad']
    df['locations']=df['locations'].str.replace('-',' ').str.strip().str.lower()
    df['emails']=df['emails'].str.strip().str.lower().replace('-','')
    df.drop(columns=['fechas_nacimiento','nombres'],inplace=True)

    df.to_csv('../files/uba_universidades_h.txt', index=False)


with DAG(
	'procesar_universidades_h_pandas',
	description='procesar_universidades_h_pandas',
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 21),
) as dag:

	tarea_1=PythonOperator(
        task_id='procesar_universidades_h_pandas',
        python_callable=procesar_universidades_h_pandas
        )
	

	tarea_1