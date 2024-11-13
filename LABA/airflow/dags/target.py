from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule = "@daily",      
    catchup=False)
def target():

    @task
    def message(dag_run=None):
        print("dag_run=", dag_run.conf.get("message"))

    message()

target()


