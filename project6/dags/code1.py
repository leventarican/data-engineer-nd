import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")

# run task daily. start from 10 days back. airflow will run task ~10x
dag = DAG(
        "lesson1.exercise2",
        start_date=datetime.datetime.now() - datetime.timedelta(days=10), schedule_interval="@daily"
)

task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)