# Project 6
https://github.com/leventarican/data-engineer-nd

* automate and monitor data warehouse ETL pipeline with Apache Airflow
* source dataset
    * JSON logs / log data (user activity)
    * JSON song data / metadata (songs the users listen to)
* stage data from S3 to Redshift (`copy` statement)
* write custom _operators_ to perform _tasks_:
    * staging data
    * filling data warehouse
    * running checks (data quality)
* add Airflow connection > Admin > Connections:
    * `aws_credentials` with `Amazon Web Services` connection type
    * `redshift` with `Postgres` connection type
* you can reuse code from project 2

## Additional Information

### Apache Airflow
* automation and monitoring for data warehouse ETL

### Example DAG's
* example 6 / `code5.py`
* task: create table in redshift
* task: load data from s3 to redshift
* task: calculate location traffic

![](dag-example6.png)

![](redshift-table.png)

### Local Airflow Installation
* installation with `pipenv`
    * install pipenv with `sudo apt install pipenv`
* open pipenv shell: `pipenv install apache-airflow`. 
* on finish you should receive the following message. 
* a `Pipfile` and `Pipefile.lock` will be created.
```
To activate this project's virtualenv, run the following:
$ pipenv shell
```

#### Nice To Know
* if you dont want to use the default dags folder, ensure to set a dags folder (where your DAG code reside)
```
~/airflow/airflow.cfg
```
* you can check the list of dags with 
```
airflow dags list
```
* use airflow cli oder UI for communicating with airflow
* start the scheduler
```
airflow scheduler
```
* optionally you can start the UI
```
airflow webserver --port 8080
```
* run a task with airflow cli (see `code6.py`)
```
$ airflow tasks test code6 task_1 now

[2021-01-23 14:00:47,537] {bash.py:158} INFO - Running command: echo hello airflow
[2021-01-23 14:00:47,545] {bash.py:169} INFO - Output:
[2021-01-23 14:00:47,547] {bash.py:173} INFO - hello airflow
```

### Links
* https://airflow.apache.org/docs/apache-airflow/stable/start.html#basic-airflow-architecture
* https://github.com/jghoman/awesome-apache-airflow
* https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/index.html
* 