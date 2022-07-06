import os
import pandas as pd
import psycopg2
from decouple import config


##### EXTRACT DATA #####

# Database connection
def connect_db():
    username = config('username')
    password = config('password')
    host = config('host')
    db = config('db')
    conn = psycopg2.connect(dbname=db, user=username, password=password, host=host)
    return conn

# Query
def query_uni(univ):
    with open(f'sql/query_{univ}.sql', 'r') as file:
        university = file.read()
        file.close()
        bbdd = connect_db()
        df = pd.read_sql(university, bbdd)
        os.makedirs('files', exist_ok=True)
        return df.to_csv(f'files/{univ}.csv', index=False)


# Python_callable
def extract_data():
    connect_db()
    query_uni()






##### CLEAN DATA #####

def clean_comahue():
    pass

def clean_salvador():
    pass


# Python_callable
def transform_data():
    clean_salvador()
    clean_comahue()






##### LOAD DATA #####

# Python_callable
def load_data():
    pass