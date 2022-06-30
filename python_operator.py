#Python Operator to process obtained data from DB

from airflow.operators.python import PythonOperator

def process_data():
    def process_moron():
        pass

    def process_cuarto():
        pass

    process_moron()
    process_cuarto()

transform_data=PythonOperator(task_id="transform", python_callable=process_data)