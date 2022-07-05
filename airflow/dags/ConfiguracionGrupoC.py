from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3Hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import pandas as pd
from pathlib import Path
import os
import csv


#  configuration of logs
def my_logs():
    logging.basicConfig(format='%(asctime)s - %(name)s  - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG)


logger = logging.getLogger(__name__)

files_path = Path(__file__).parent.resolve()

filename = ['Universidad_Nacional_de_Jujuy', 'Universidad_de_Palermo']

def load_s3():
   
    for uni in filename:
        hook = S3Hook(aws_conn_id="my_conn")
        hook.load_file(
        filename = f'/{files_path}/resources/{uni}.txt',
        key = f'{uni}.txt',
        bucket_name = "cohorte-junio-a192d78b",
        replace=True,
    )

def transform_string(columna):
    try:
        columna.apply(lambda x:str(x).lower())
        columna.apply(lambda x:str(x).replace('-', ' '))
        columna.apply(lambda x:str(x).replace('_', ' '))
        columna.apply(lambda x:str(x).strip())
    except:
        logger.error('Error al normalizar los caracteres')  
    return columna         

def  normalize_data():

    for uni in filename:

        df_univ = pd.read_csv(f"{files_path}/files/{uni}.csv")

        # university: str minúsculas, sin espacios extras, ni guiones
        df_univ["university"] = transform_string(df_univ["university"])


        # career: str minúsculas, sin espacios extras, ni guiones
        df_univ["career"] = transform_string(df_univ["career"])
        
        # inscription_date: str %Y-%m-%d format
        old_date = pd.to_datetime(df_univ["inscription_date"])
        df_univ["inscription_date"] = pd.to_datetime(old_date, "%Y-%m-%d")

        # first_name: str minúscula y sin espacios, ni guiones
        # last_name: str minúscula y sin espacios, ni guiones
        df_univ["full_name"] = transform_string(df_univ["full_name"])
    
        try:
            df_univ["first_name"] = df_univ["full_name"].apply(lambda x: str(x).split(" ")[0])
            df_univ["last_name"] = df_univ["full_name"].apply(lambda x: str(x).split(" ")[1])
        except:
            df_univ["first_name"] = "null"
            df_univ["last_name"] = df_univ["full_name"]
        

        # gender: str choice(male, female)
        df_univ["gender"] = transform_string(df_univ["gender"])
        dict_gender = {"f": "female", "m": "male"}
        df_univ["gender"] = df_univ["gender"].map(dict_gender)

        # age: int
        today = datetime.now()
        df_univ['age'] = df_univ['birth_date'].apply(lambda x:(today.year - datetime.strptime(str(x),'%Y-%m-%d').year))
        
        # location: str minúscula sin espacios extras, ni guiones
        if uni == 'Universidad_Nacional_de_Jujuy' :
         
            df_univ["location"] = df_univ["locations"]
            df_cp = pd.read_csv(f"{files_path}/assets/codigos_postales.csv")
            df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
            dict_cp = dict(zip(df_cp["localidad"],df_cp["codigo_postal"]))
            df_univ["postal_code"] = df_univ["location"].apply(lambda x: dict_cp[x])
        else:
            df_univ["postal_code"] = df_univ["postal_code"]
            df_cp = pd.read_csv(f"{files_path}/assets/codigos_postales.csv")
            df_cp["localidad"] = df_cp["localidad"].apply(lambda x: x.lower())
            dict_cp = dict(zip(df_cp["codigo_postal"], df_cp["localidad"]))
            df_univ["location"] = df_univ["postal_code"].apply(lambda x: dict_cp[x])
        
        # email: str minúsculas, sin espacios extras, ni guiones
        df_univ["email"] = transform_string(df_univ["email"])

        # Guardando información necesaria en un .txt
        df_univ = df_univ[
            [
                "university",
                "career",
                "inscription_date",
                "first_name",
                "last_name",
                "gender",
                "age",
                "postal_code",
                "location",
                "email",
            ]
        ]
        os.makedirs(f"{files_path}/resources", exist_ok=True)
        df_univ.to_csv(f"{files_path}/resources/{uni}.txt", sep="\t")
    


def  extract_data_Jujuy():
    
    with open(f'{files_path}/sql_query/Universidad_Nacional_de_Jujuy.sql', 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()

    pg_hook = PostgresHook(postgres_conn_id='db_universidades')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    
    os.makedirs(f"{files_path}/files", exist_ok=True)
    columns = [i[0] for i in cursor.description]

    csv_file = (f"{files_path}/files/Universidad_Nacional_de_Jujuy.csv")
    # Create csv file
    with open(csv_file, mode="w") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(columns)
        for source in sources:
            writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])
    

def  extract_data_Palermo():
   
    with open(f'{files_path}/sql_query/Universidad_de_Palermo.sql', 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()

    pg_hook = PostgresHook(postgres_conn_id='db_universidades')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    
    os.makedirs(f"{files_path}/files", exist_ok=True)
    columns = [i[0] for i in cursor.description]

    csv_file = (f"{files_path}/files/Universidad_de_Palermo.csv")
    # Create csv file
    with open(csv_file, mode="w") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(columns)
        for source in sources:
            writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])


def extract_data():
   extract_data_Jujuy()
   extract_data_Palermo()


# configuration of retries
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    'DAG_UniversidadesGrupoC',
    description='DAG para el grupo Universidades C',
    default_args=default_args,
    schedule_interval="@hourly", 
    start_date=datetime(2022, 6, 23)
    
    ) as dag:
    # tarea de logging
    logging_task = PythonOperator(task_id='my_logs', python_callable=my_logs)
    process_task =  PythonOperator(task_id='extract_data', python_callable=extract_data)
    # tarea de normalizacion
    transform_task = PythonOperator(task_id='normalize_data', python_callable=normalize_data)
    load_task = PythonOperator(task_id='load_s3', python_callable=load_s3)
    
    logging_task >> process_task >> transform_task >> load_task