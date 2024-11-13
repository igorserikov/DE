from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset

data_a = Dataset("s3://bucket1")
data_b = Dataset("s3://bucket2")

@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule_interval="@daily",      # Расписание, например, ежедневно
    catchup=False)
def producer():

    @task(outlets=[data_a])
    def update_a():
        print("update A")

    @task(outlets=[data_b])
    def update_b():
        print("update B")

    update_a()>>update_b()

producer()
