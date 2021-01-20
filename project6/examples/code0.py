import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# a basic example for apache airflow

#
# a function for the PythonOperator
#
def my_function():
    logging.info("python task lesson 1 exercise 1")


dag = DAG(
    'lesson1.exercise1',
    start_date=datetime.datetime.now())

greet_task = PythonOperator(
    task_id="l1e1",
    python_callable=my_function,
    dag=dag
)
