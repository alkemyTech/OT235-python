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
        cursor.execute(sql_query)
        s=cursor.fetchall()
        df = pd.DataFrame(s)
        df.to_csv(f'exported_data.csv',index=True,header=False)
        print(f'Successful Connection')

    except Exception as e:
        print(e)

    finally:
        cursor.close()
        connection.close()
        print('Conexion finalizada')

#DAG Operator
extract_sql=PythonOperator(task_id="extract", python_callable=extract)