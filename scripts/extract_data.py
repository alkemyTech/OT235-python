from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from pathlib import Path


def extract_from_db() -> None:
    """
    Callable function for extract task in DAG 'universidades_grupo_E_etl'.
    Extracts data from two universities (group E)
    and saves them in csv files.
    """
    # Connection to database
    pg_hook = PostgresHook(postgres_conn_id='postgres_univ',
                           schema='training'
                           )
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # File paths for sql files
    DIR = Path(__file__).resolve().parent.parent
    query_file_names = ['/query_grupo_E_interam.sql',
                        '/query_grupo_E_pampa.sql']
    query_file_paths = [str(DIR) + file for file in query_file_names]

    # data extraction and data saving
    for query_file in query_file_paths:
        with open(query_file) as query:
            cur.execute(query.read())
            data = pd.DataFrame(cur.fetchall())

            # file path creation for local data storage
            local_path = str(DIR) + '/files'
            univ = query_file.split('_')[-1].split('.')[0]
            file_name = f'/datos_universidad_{univ}.csv'
            local_file_path = local_path + file_name

            # folder creation
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            # column names extracted from sql query
            col_names = [desc[0] for desc in cur.description]

            data.to_csv(local_file_path, header=col_names, encoding='latin1')

    cur.close()
