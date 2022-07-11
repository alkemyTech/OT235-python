from airflow import DAG
from pathlib import Path
from dagfactory import dagfactory

files_path = Path(__file__).parent.resolve()
config_file = f"{files_path}/dag_factory.yaml"
dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())