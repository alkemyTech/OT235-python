from airflow import DAG
from datetime import timedelta
import dagfactory

#Path where the yml for the creation of dags are located
config_file = '/home/cris/airflow/dags/dags-config/dags_config_uni_E.yml'
dag_factory = dagfactory.DagFactory(config_file)
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())


