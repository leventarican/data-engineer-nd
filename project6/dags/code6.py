import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="code6",
    tags=["code"],
    start_date=datetime.datetime.now())

t0 = BashOperator(
    task_id='task_0',
    bash_command='echo 0',
    dag=dag,
)

t1 = BashOperator(
    task_id='task_1',
    bash_command='echo hello airflow',
    dag=dag,
)

t0 >> t1
