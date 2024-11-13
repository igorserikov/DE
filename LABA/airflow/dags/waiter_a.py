from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset
from airflow.sensors.external_task import ExternalTaskSensor


@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule = "@daily",      
    catchup=False)
def waiting_for_a():

    waiting = ExternalTaskSensor(
        task_id="waiting_for_a",
        external_dag_id="dag_a",
        external_task_id="task_a"
    )
        
    waiting

waiting_for_a()
