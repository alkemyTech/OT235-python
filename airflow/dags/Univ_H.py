from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import pandas as pd
from pathlib import Path
import os
import csv


#  configuracion de logs
def my_logs():
    logging.basicConfig(format='%(asctime)s - %(name)s  - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG)


log = logging.getLogger(__name__)

files_path = Path(__file__).parent.resolve()

filename = ['Universidad_Nacional_de_Jujuy', 'Universidad_de_Palermo']

# funcion para cargar los archivos .txt en S3
def load_s3():
   
    for uni in filename:
        log.info(f'Se carga el archivo {uni}.txt al repositorio S3')
        hook = S3Hook(aws_conn_id="my_conn")
        hook.load_file(
        filename = f'/{files_path}/resources/{uni}.txt',
        key = f'{uni}.txt',
        bucket_name = "cohorte-junio-a192d78b",
        replace=True,
    )

# funcion para normalizar los strings
def transform_string(columna):
    try:
        columna.apply(lambda x:str(x).lower())
        columna.apply(lambda x:str(x).replace('-', ' '))
        columna.apply(lambda x:str(x).replace('_', ' '))
        columna.apply(lambda x:str(x).strip())
    except:
        log.error('Error al normalizar los caracteres')  
    return columna         

def  normalize_data():
    log.info('Comenzando la normalizacion')
    for uni in filename:
        log.info(f'Se normaliza el archivo {uni}.csv')
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
        abreviations = {'mr.': '','dr.': '','mrs.': '','ms.': '','md': '','dds': '','jr.': '','dvm': '','phd': ''}

        df_univ["full_name"] = transform_string(df_univ["full_name"])
    
        try:
            df_univ["first_name"] = df_univ["full_name"].apply(lambda x: str(x).split(" ")[0])
            df_univ["last_name"] = df_univ["full_name"].apply(lambda x: str(x).split(" ")[1])
            for abreviation,black in abreviations.items():
                df_univ['last_name'] =  df_univ['last_name'].apply(lambda x: x.replace(abreviation,black))
        except:
            df_univ["first_name"] = "null"
            df_univ["last_name"] = df_univ["full_name"]
            for abreviation,black in abreviations.items():
                df_univ['last_name'] =  df_univ['last_name'].apply(lambda x: x.replace(abreviation,black))

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
        # se crea la carpeta resources
        os.makedirs(f"{files_path}/resources", exist_ok=True)
        # se graba el archivo .txt los datos normalizados
        df_univ.to_csv(f"{files_path}/resources/{uni}.txt", sep="\t")
    


def  extract_data_Jujuy():
     # Se se lee el archivo .sql
    with open(f'{files_path}/sql_query/Universidad_Nacional_de_Jujuy.sql', 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()
    # se establece la conexion con la base de datos
    try:
        pg_hook = PostgresHook(postgres_conn_id='db_universidades')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        sources = cursor.fetchall()
        
        #se crea la carpeta files
        os.makedirs(f"{files_path}/files", exist_ok=True)
        columns = [i[0] for i in cursor.description]

        csv_file = (f"{files_path}/files/Universidad_Nacional_de_Jujuy.csv")
        # se crea el CSV
        with open(csv_file, mode="w") as file:
            writer = csv.writer(file, delimiter=",")
            writer.writerow(columns)
            for source in sources:
                writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])
    except:  
        log.error('Error al establecer la conexion a la base de datos')
 

def  extract_data_Palermo():
    
    # Se se lee el archivo .sql
    with open(f'{files_path}/sql_query/Universidad_de_Palermo.sql', 'r', encoding='utf-8') as f:
        request = f.read()
        f.close()
    try:
        # se establece la conexion con la base de datos
        pg_hook = PostgresHook(postgres_conn_id='db_universidades')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        sources = cursor.fetchall()
        
        #se crea la carpeta files
        os.makedirs(f"{files_path}/files", exist_ok=True)
        columns = [i[0] for i in cursor.description]

        csv_file = (f"{files_path}/files/Universidad_de_Palermo.csv")
        # se crea el CSV
        with open(csv_file, mode="w") as file:
            writer = csv.writer(file, delimiter=",")
            writer.writerow(columns)
            for source in sources:
                writer.writerow([source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7]])
    except:
       log.error('Error al establecer la conexion a la base de datos')
  


def extract_data():
   extract_data_Jujuy()
   extract_data_Palermo()