import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# a demo for task dependencies

def hello_world():
    logging.info("Hello World")


def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")


dag = DAG(
    "lesson1.exercise3",
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

add_task = PythonOperator(
    task_id="add",
    python_callable=addition,
    dag=dag)

sub_task = PythonOperator(
    task_id="sub",
    python_callable=subtraction,
    dag=dag)

div_task = PythonOperator(
    task_id="div",
    python_callable=division,
    dag=dag)

#
# task dependencies dag (graph):
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task
hello_world_task >> add_task
hello_world_task >> sub_task
add_task >> div_task
sub_task >> div_task