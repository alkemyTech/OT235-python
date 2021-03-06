import logging
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from scripts.scrape import extract_data
from scripts.process import process_flores, process_v_maria

""" 
    Log configuration
"""
#Format Settings
format= '%(asctime)s - %(name)s - %(message)s'

#Logging 
logging.basicConfig(
level= logging.INFO, 
datefmt= '%y-%m-%d', 
format= format)

#Logger name
logger= logging.getLogger('Universidades_A')

#Logger message
logger.info('ETL_for_universities')


"""
Default arguments of each of our tasks.
    owner: task owner
    depends_on_past: dependency on other tasks
    email: Mail from responsible persons
    email_on_failure: Send mail on failure
    email_on_retry: Send mail if a task is retried
    retries: Attempts in case the task fails
    retry_delay: minutes of attempts between retries

"""
default_args={
    'owner': 'Cristian Rosas',
    'depends_on_past': False,
    'email': ['rosascristian26@gmail.com'],
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
}


#Define functions

def scrape():
    extract_data()

def process():
    process_flores()
    process_v_maria()


def save():
    pass


#Define dag

"""
Arguments dag receives

dag name: String
default dictionary arguments: Dictionary
description: String
Agenda of when we want it to be executed: time
When it starts: days_ago()
tags= list

"""
with DAG(
    'Universidades_a',
    default_args= default_args,
    description= 'Dag for A universities',
    schedule_interval= timedelta(hours=1),
    start_date= days_ago(2),
    tags= ['Universidad De Flores','Universidad Nacional De Villa María'],
) as dag:

    #define tasks

    scrape_task= PythonOperator(task_id= 'scrape',
                                python_callable= scrape, 
                                dag= dag, 
                                retries= 5)

    process_task= PythonOperator(task_id= 'process', 
                                 python_callable= process, 
                                 dag= dag, 
                                 retries= 5)

    save_task= PythonOperator(task_id= 'save', 
                              python_callable= save, 
                              dag= dag, 
                              retries= 5)

    #Dependency between tasks/order

    scrape_task >> process_task >> save_task