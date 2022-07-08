''' Creation of dynamic dag using dagfactory library
    for ETL process of universities in group A
    DAG configuration in yaml file.
'''
from airflow import DAG
import dagfactory
from pathlib import Path


CONFIG_FILE = '/dag_config_group_A.yaml'

DIR = Path(__file__).resolve().parent
path = str(DIR) + CONFIG_FILE

dynamic_dag_group_A = dagfactory.DagFactory(path)

dynamic_dag_group_A.clean_dags(globals())
dynamic_dag_group_A.generate_dags(globals())
