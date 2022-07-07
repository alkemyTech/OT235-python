from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging
import psycopg2
import pandas as pd
import datetime
from decouple import config
import os

"""
DAG Description:

DAG to obtain data for universities of group F from SQL database
"""

#logging config
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%m-%d',
    format='%(asctime)s: %(levelname)s - %(message)s',
)

def query_universities_f():
    #conexión db
    logging.info('Conectando a la DB')
    conn = psycopg2.connect(
                host=config("HOST"),
                user=config("USER"),
                password=config("PASSWORD"),
                database=config("DATABASE")
    )

    #cursor
    cur = conn.cursor()

    #definición de queries
    with open("query_universities_f.sql", encoding='utf-8') as query_file:
        content = query_file.read()
        queries = content.split("-- Universidad Nacional De Río Cuarto")

    #ejecuta queries
    logging.info('Ejecutando las consultas a la DB')
    cur.execute(queries[0])
    result_moron = cur.fetchall()

    cur.execute(queries[1])
    result_cuarto = cur.fetchall()

    #resultados de queries a csv
    df_moron = pd.DataFrame(result_moron)
    df_cuarto = pd.DataFrame(result_cuarto)

    logging.info('Guardando los resultados')
    
    #crea carpeta para guardar
    outdir = './files'
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    #guarda archivos resultantes
    outname = 'universidad_moron.csv'
    fullname = os.path.join(outdir, outname)
    df_moron.to_csv(fullname)

    outname = 'universidad_cuarto.csv'
    fullname = os.path.join(outdir, outname)
    df_cuarto.to_csv(fullname)

    logging.info('Consultas finalizadas')

    cur.close()
    conn.close()
    logging.info('Conexión con la DB cerrada')



default_args = {
    'owner': 'guidocaru',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
	'sql_operator',
	description='get universities of group f from sql',
    default_args=default_args,
	schedule_interval='@hourly',
	start_date=datetime(2022, 6, 29),
) as dag:
	get_data = PythonOperator(
        task_id ='get data from sql',
        python_callable = query_universities_f
    )

	get_data


