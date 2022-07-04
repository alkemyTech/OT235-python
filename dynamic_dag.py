from airflow import DAG
import dagfactory
from pathlib import Path

"""Create Yml path and create DAG from here"""
yml_path = Path(r"C:\Users\lugon\OneDrive\Escritorio\acceleracion_alkemy\OT235-python\config_file.yml")
dag_factory = dagfactory.DagFactory(yml_path)


dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())