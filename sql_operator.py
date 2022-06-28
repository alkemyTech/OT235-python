import psycopg2
import pandas

"""Function to extract data to sql"""
def extract():
    try:
        connection=psycopg2.connect(
                host='training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com',
                user='alkymer2',
                password='Alkemy23',
                database='training'
        )

        cursor=connection.cursor()
        f_sql = open('consulta.sql','r',encoding='utf-8')
        sql_query=f_sql.read()
        df=pd.read_sql(sql_query, connection)
        df.to_csv(f'exported_data.csv',index=False)


    except Exception as e:
        print(e)

    finally:
        cursor.close()
        connection.close()
        print('Conexion finalizada')

#DAG Operator
extract_sql=PythonOperator(task_id="extract", python_callable=extract)