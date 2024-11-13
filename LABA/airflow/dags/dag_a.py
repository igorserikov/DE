from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule = "@daily",      
    catchup=False)
def dag_a():

    @task
    def task_a():
        print("task_a")

    task_a()

dag_a()
