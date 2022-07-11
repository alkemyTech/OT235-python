from dag_factory import DAGFactory
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy import text
from dateutil.relativedelta import relativedelta

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')

# add formatter to ch
ch.setFormatter(formatter)

u_cine_logger = logging.getLogger('u_cine_logger')
u_cine_logger.setLevel(logging.DEBUG)

uba_logger = logging.getLogger('uba_logger')
uba_logger.setLevel(logging.DEBUG)

# add ch to logger
u_cine_logger.addHandler(ch)
uba_logger.addHandler(ch)


def obtener_del_sql():

# Creo la conexion
    engine = create_engine('postgresql://alkymer2:Alkemy23@training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com:5432/training')

  #Genero la conexion, abro el archivo de la query sql y la guardo en query_universidad
    with engine.connect() as con:
        with open("/home/r580/Downloads/airflow/universidades_c.sql") as file:
            query = file.read()
            query_universidad=query.split("#")

    #Leo del sql y guardo en dataframe pandas
            df_u_cine = pd.read_sql(query_universidad[0], con)

    #Guardo en files en csv
            df_u_cine.to_csv("/home/r580/Downloads/airflow/files/1_universidades_c.csv", index=False)

            #u_cine_logger.info('Consulta Universidad Del Cine')

    #Leo del sql y guardo en dataframe pandas
            df_uba = pd.read_sql(query_universidad[1], con)
    
    #Guardo en files en csv
            df_uba.to_csv("/home/r580/Downloads/airflow/files/2_universidades_c.csv", index=False)

            #uba_logger.info('Consulta UBA')



def procesar_universidades_c_pandas():

    process_ucine()
    
    process_uba()

def process_ucine():
 
    df =pd.read_csv('/home/r580/Downloads/airflow/files/1_universidades_c.csv',sep=",")
    codigos_postales =pd.read_csv('/home/r580/Downloads/airflow/files/codigos_postales.csv',sep=",")

    df['university']=df['university'].str.lower().str.strip().replace('-','')
    df['career']=df['career'].str.strip().str.lower().replace('-','')
    df['inscription_date']=pd.to_datetime(df['inscription_date'], format='%Y-%m-%d', infer_datetime_format=True)
    reemplazar_caracteres=['miss ','ms. ','mr. ',' dvm',' dds','mrs. ',' md','dr. ',' md',' dds',' jr.',' v',' ii',' phd',' iv',' iii']
    for a in reemplazar_caracteres:
        df['nombre']=df['nombre'].str.replace(a,'')
    df2=df['nombre'].str.strip().str.lower().str.split(' ',expand=True)
    df[['first_name','last_name']]=df2[[0,1]]
    df['sexo']=df['sexo'].str.replace('f','female').replace('m','male')
    df['birth_date']=pd.to_datetime(df['birth_date'], format='%Y-%m-%d', infer_datetime_format=True)
    df['age']=df['birth_date'].map(lambda x : abs(relativedelta(date.today(),x)).years)
    df['location']=df['location'].str.replace('-',' ').str.upper()
    df3=pd.merge(df['location'],codigos_postales,how='left',left_on=['location'],right_on=['localidad'])
    df['postal_code']=df3['codigo_postal']
    df['location']=df['location'].str.lower()
    df['email']=df['email'].str.strip().str.lower().replace('-','')
    df.drop(columns=['birth_date','nombre'],inplace=True)

    df.to_csv('/home/r580/Downloads/airflow/files/1_universidades_c.txt', index=False)    


def process_uba():

    df =pd.read_csv('/home/r580/Downloads/airflow/files/2_universidades_c.csv',sep=",")
    codigos_postales =pd.read_csv('/home/r580/Downloads/airflow/files/codigos_postales.csv',sep=",")

    df['universidad']=df['universidad'].str.lower().str.strip().replace('-','')
    df['careers']=df['careers'].str.strip().str.lower().replace('-','')
    df['fecha_de_inscripcion']=pd.to_datetime(df['fecha_de_inscripcion'], format='%d-%b-%Y', infer_datetime_format=True)
    reemplazar_caracteres=['miss_','ms._','mr._','_dvm','_dds','mrs._','_md','dr._','_md','_dds','_jr.','_v','_ii','_phd','_iv','_iii']
    for a in reemplazar_caracteres:
        df['names']=df['names'].str.replace(a,'')
    df2=df['names'].str.strip().str.lower().str.split('_',expand=True)
    print(df2)
    df[['first_name','last_name']]=df2[[0,1]]
    df['sexo']=df['sexo'].str.replace('f','female').replace('m','male')
    df['birth_dates']=df['birth_dates'].str.replace('00-','01-').replace('29-Feb-07','28-Feb-07')\
    .replace('31-Sep-20','30-Sep-20').replace('31-Apr-16','30-Apr-16').replace('31-Feb-14','28-Feb-14')\
    .replace('29-Feb-06','28-Feb-06').replace('31-Sep-09','30-Sep-09').replace('30-Feb-16','29-Feb-16')\
    .replace('31-Apr-04','30-Apr-04').replace('31-Sep-02','30-Sep-02').replace('31-Nov-09','30-Nov-09')\
    .replace('31-Nov-29','30-Nov-29').replace('30-Feb-18','28-Feb-18').replace('31-Jun-15','30-Jun-15')\
    .replace('31-Apr-10','30-Apr-10').replace('31-Sep-07','30-Sep-07')
    df['birth_dates']=pd.to_datetime(df['birth_dates'], format='%d-%b-%Y', infer_datetime_format=True)
    df['age']=df['birth_dates'].map(lambda x : abs(relativedelta(date.today(),x)).years)
    df3=pd.merge(df['codigo_postal'],codigos_postales,how='left',left_on=['codigo_postal'],right_on=['codigo_postal'])
    df['locations']=df3['localidad']
    df['locations']=df['locations'].str.replace('-',' ').str.strip().str.lower()
    df['correos_electronicos']=df['correos_electronicos'].str.strip().str.lower().replace('-','')
    df.drop(columns=['birth_dates','names'],inplace=True)

    df.to_csv('/home/r580/Downloads/airflow/files/2_universidades_c.txt', index=False)


#Genero el hook para subir el archivo 1 al bucket
def subir_al_s3_universidades_c_1(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id='s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

#Genero el hook para subir el archivo 2 al bucket
def subir_al_s3_universidades_c_2(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id='s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

tasks = {}
op_kwa= {}
# say_hi has no dependencies, set to []
tasks[obtener_del_sql] = []
op_kwa[obtener_del_sql] = []

tasks[procesar_universidades_c_pandas] = [obtener_del_sql]
op_kwa[procesar_universidades_c_pandas] = []
# the other 2 tasks depend on say_hi
tasks[subir_al_s3_universidades_c_1] = [procesar_universidades_c_pandas]
op_kwa[subir_al_s3_universidades_c_1] = {'filename':'/home/r580/Downloads/airflow/files/1_universidades_c.txt','key':'1_universidades_c.txt','bucket_name':'cohorte-junio-a192d78b'}
tasks[subir_al_s3_universidades_c_2] = [procesar_universidades_c_pandas]
op_kwa[subir_al_s3_universidades_c_2] = {'filename':'/home/r580/Downloads/airflow/files/2_universidades_c.txt','key':'2_universidades_c.txt','bucket_name':'cohorte-junio-a192d78b'}
   
DAG_NAME = 'Dag_dinamico_Universidades_C'

override_args = {
    'owner': 'Alkemy',
    'retries': 5
}

dag = DAGFactory().get_airflow_dag(DAG_NAME, tasks, op_kwa, default_args=override_args, cron='@hourly')