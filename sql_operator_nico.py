import psycopg2
import pandas as pd

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
        df.to_csv(f'kennedy_data.csv',index=False)


    except Exception as e:
        print(e)

    finally:
        cursor.close()
        connection.close()
        print('Conexion finalizada')

extract()
