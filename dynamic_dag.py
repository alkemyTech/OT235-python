from airflow import DAG
import dagfactory
from pathlib import Path

"""Create Yml path and create DAG from here"""
yml_path_1= Path(r'/home/lnicolas/acceleracion_alkemy/config_file.yml')


dag_factory = dagfactory.DagFactory(yml_path)


dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())