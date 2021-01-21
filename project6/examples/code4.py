# Instructions
# Use the Airflow context in the pythonoperator to complete the TODOs below. Once you are done, run your DAG and check the logs to see the context in use.

import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details(*args, **kwargs):
    #
    # TODO: Extract ds, run_id, prev_ds, and next_ds from the kwargs, and log them
    # NOTE: Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/macros.html
    #
    ds = kwargs['ds']
    run_id = kwargs['run_id']
    previous_ds = kwargs.get('previous_ds')
    next_ds = kwargs.get('next_ds')

    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")
#[2021-01-21 22:21:16,311] {logging_mixin.py:95} INFO - [2021-01-21 22:21:16,311] {exercise5.py:24} INFO - Execution date is 2021-01-21
#[2021-01-21 22:21:16,311] {logging_mixin.py:95} INFO - [2021-01-21 22:21:16,311] {exercise5.py:25} INFO - My run id is manual__2021-01-21T22:21:05.260578+00:00
#[2021-01-21 22:21:16,312] {logging_mixin.py:95} INFO - [2021-01-21 22:21:16,311] {exercise5.py:29} INFO - My next run will be 2021-01-21

dag = DAG(
    'lesson1.exercise5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)
