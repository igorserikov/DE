from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset

data_a = Dataset("s3://bucket1")
data_b = Dataset("s3://bucket2")

@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule = [data_a, data_b],      
    catchup=False)
def consumer():

    @task
    def run():
        print("run")

    run()

consumer()
