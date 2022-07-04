"""
This file holds the DAG Factory
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


class DAGFactory:
    """
    Class that provides useful method to build an Airflow DAG
    """

    @classmethod
    def create_dag(cls, dagname, default_args={}, catchup=False, concurrency=5, cron=None):
        """
        params:
            dagname(str): the name of the dag
            default_args(dict): a dict with the specific keys you want to edit from the original DEFAULT_ARGS
            catchup(bool): Perform scheduler catchup (or only run latest)? Defaults to True
            concurrency(int): the number of task instances allowed to run concurrently
            cron(str): the cron expression or the schedule
        returns:
            DAG object
        """
        DEFAULT_ARGS = {
            'owner': 'Data Engineer',
            'depends_on_past': False,
            'start_date': datetime(2021, 1, 1),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

        DEFAULT_ARGS.update(default_args)
        dagargs = {
            'default_args': DEFAULT_ARGS,
            'schedule_interval': cron,
            'catchup': catchup,
            'concurrency': concurrency
        }

        dag = DAG(dagname, **dagargs)
        return dag

    @classmethod
    def add_tasks_to_dag(cls, dag, tasks,op_kwa):
        """
        Adds tasks to DAG object, sets upstream for each task.
        params:
            dag(DAG)
            tasks(dict): dictionary in which each key is a callback. The value of that key is the task's dependencies.
            If a task has no dependencies (it's the first task), set an empty list [] as the value.
            IMPORTANT: all tasks have to be there even if they don't have dependencies
        returns:
            dag(DAG) with tasks
        """
        with dag as dag:
            aux_dict = {}

            # create task objects and store them in a dictionary of "func name": task
            for func in tasks:
                task_id = func.__name__
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=func,
                    dag=dag,
                    op_kwargs=dict(op_kwa[func])
                )
                aux_dict[task_id] = task

            # for each task, set up the tasks predecessors
            for func, dependencies in tasks.items():
                task_id = func.__name__
                # does not have dependencies? then it's the first task
                for dep in dependencies:
                    aux_dict[dep.__name__] >> aux_dict[task_id]

        return dag

    @classmethod
    def get_airflow_dag(cls, dagname, tasks, op_kwa, default_args={}, catchup=False, concurrency=5, cron=None):
        """
        The actual method that has to be called by a DAG file to get the dag.
        params:
            idem as create_dag + add_tasks_to_dag
        returns:
            DAG object
        """
        dag = cls.create_dag(dagname, default_args=default_args, catchup=catchup, concurrency=concurrency, cron=cron)
        dag = cls.add_tasks_to_dag(dag, tasks,op_kwa)
        return dag