from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(start_date=datetime(2023, 1, 1), # Дата начала (обязательно в прошлом для запуска при тестировании)
    schedule = "@daily",      
    catchup=False)
def controller():

    @task
    def start():
        print("start")

    trigger = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id = 'target',
        conf={"message":"my_data"}
    )

    @task
    def finish():
        print("finish")

    start() >> trigger >> finish()


controller()
