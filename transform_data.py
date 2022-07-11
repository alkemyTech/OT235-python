from airflow.operators.python import PythonOperator
import pandas as pd
import datetime

"""
Funciones para procesar datos obtenidos de la consulta a la BD
para las universidades del grupo F (Río Cuarto y Morón)
"""


def process_data():

    def process_rio_cuarto():

        df_cuarto = pd.read_csv("files/universidad_cuarto.csv") 

        #elimina primera columna (indices)
        df_cuarto = df_cuarto.iloc[: , 1:]

        #nuevos nombres de columnas
        df_cuarto.columns = ["university", "career", "inscription_date", "name", "gender", "age", "location", "email"] 

        #minusculas y elimina guiones de university
        df_cuarto["university"] = df_cuarto["university"].str.replace("-"," ")
        df_cuarto["university"] = df_cuarto["university"].str.lower()

        #elimina guiones y espacios de career
        df_cuarto["career"] = df_cuarto["career"].str.replace("-"," ")
        df_cuarto["career"] = df_cuarto["career"].str.strip()

        #formatea date
        df_cuarto["inscription_date"] =  pd.to_datetime(df_cuarto["inscription_date"], format='%d/%b/%y')
        df_cuarto["inscription_date"] =  pd.to_datetime(df_cuarto["inscription_date"], format='%Y/%b/%d')
        df_cuarto['inscription_date'] = df_cuarto['inscription_date'].dt.strftime('%Y-%b-%d')

        #separa columna de nombres
        names = df_cuarto["name"].str.split("-", n=1, expand=True)

        #agrega first y last name al df
        df_cuarto["first_name"] = names[0]
        df_cuarto["last_name"] = names[1]
        df_cuarto.drop('name', inplace=True, axis=1)

        #edita columna gender
        df_cuarto.loc[(df_cuarto["gender"] == "M"), "gender"] = "male"
        df_cuarto.loc[(df_cuarto["gender"] == "F"), "gender"] = "female"

        #minúscula y saca guiones de columna location
        df_cuarto["location"] = df_cuarto["location"].str.lower()
        df_cuarto["location"] = df_cuarto["location"].str.replace("-"," ")

        #lee y prepara codigos postales para merge
        cod_postales = pd.read_csv("codigos_postales.csv", encoding="utf-8")
        cod_postales.rename(columns = {'localidad':'location', 'codigo_postal':'postal_code'}, inplace = True)
        cod_postales["location"] = cod_postales["location"].str.lower()

        #merge
        df_cuarto = df_cuarto.merge(cod_postales, on="location")

        #minúscula de mail column
        df_cuarto["email"] = df_cuarto["email"].str.lower()

        #format age column

        #to datetime
        df_cuarto["age"] =  pd.to_datetime(df_cuarto["age"], format='%y/%b/%d')

        #age = today - birthdate
        df_cuarto["age"] = pd.Timestamp.now().normalize() - df_cuarto["age"]

        #datetime days to int
        df_cuarto["age"] = df_cuarto["age"].dt.days

        #edad en días dividido el promedio de días en un año
        df_cuarto["age"] = df_cuarto["age"] / 365.2425

        #float to int
        df_cuarto["age"] = df_cuarto["age"].astype(int)

    def process_moron():

        df_moron = pd.read_csv("files/universidad_moron.csv")

        #elimina primera columna (indices)
        df_moron = df_moron.iloc[: , 1:]

        #nuevos nombres de columnas
        df_moron.columns = ["university", "career", "inscription_date", "name", "gender", "age", "postal_code", "email"]

        #minusculas y elimina guiones de university
        df_moron["university"] = df_moron["university"].str.lower()
        df_moron["university"] = df_moron["university"].str.strip()

        #elimina guiones y espacios de career
        df_moron["career"] = df_moron["career"].str.lower()
        df_moron["career"] = df_moron["career"].str.strip()

        #formatea date
        df_moron["inscription_date"] =  pd.to_datetime(df_moron["inscription_date"], format='%d/%m/%Y')
        df_moron["inscription_date"] =  pd.to_datetime(df_moron["inscription_date"], format='%Y/%b/%d')
        df_moron['inscription_date'] = df_moron['inscription_date'].dt.strftime('%Y-%b-%d')

        #separa columna de nombres
        names = df_moron["name"].str.split(" ", n=1, expand=True)

        #agrega first y last name al df
        df_moron["first_name"] = names[0]
        df_moron["last_name"] = names[1]
        df_moron.drop('name', inplace=True, axis=1)

        #edita columna gender
        df_moron.loc[(df_moron["gender"] == "M"), "gender"] = "male"
        df_moron.loc[(df_moron["gender"] == "F"), "gender"] = "female"

        #lee y prepara codigos postales para merge
        df_moron["postal_code"] = df_moron["postal_code"].astype(int)
        cod_postales = pd.read_csv("codigos_postales.csv", encoding="utf-8")
        cod_postales.rename(columns = {'localidad':'location', 'codigo_postal':'postal_code'}, inplace = True)
        cod_postales["location"] = cod_postales["location"].str.lower()

        #merge
        df_moron = df_moron.merge(cod_postales, on="postal_code")

        #minúscula de mail column
        df_moron["email"] = df_moron["email"].str.lower()

        #format age column

        #to datetime
        df_moron["age"] =  pd.to_datetime(df_moron["age"], format='%d/%m/%Y')

        #age = today - birthdate
        df_moron["age"] = pd.Timestamp.now().normalize() - df_moron["age"]

        #datetime days to int
        df_moron["age"] = df_moron["age"].dt.days

        #edad en días dividido el promedio de días en un año
        df_moron["age"] = df_moron["age"] / 365.2425

        #float to int
        df_moron["age"] = df_moron["age"].astype(int)

    process_moron()
    process_rio_cuarto()

transform_data = PythonOperator(
    task_id="transform",
    python_callable=process_data
)