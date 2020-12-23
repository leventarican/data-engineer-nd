# Project 3

## Summary
* load data from S3 to staging tables on Redshift
* create the analytics tables from these staging tables

Comments are used effectively and each function has a docstring.

## How To Run the Project

## Project Repository files
* `create_tables.py`: connects to AWS Redshift (postgreSQL), drop tables, create tables
* `sql_queries.py`: kind of a DDL
    * create staging tables
    * create fact and dimension tables
* `etl.py`: here happens the magic: read json files, process and load (to db).

## Additional Information

### AWS

#### AWS SDK
* python: _boto3_
* create one IAM user for the connection

### Examples
* for the examples the sakila / pagila (MySQL, Postgres) sample database is used
* do some _data analysis_ to find some _insights_

#### 1. Work on a 3NF schema
![3NF-schema](3NF-schema.png)

#### 2. Work on a star schema
* composed with _fact_ and _dimension_ tables

![star-schema](star-schema.png)

### Links
* https://dev.mysql.com/doc/sakila/en/sakila-structure.html
* https://github.com/devrimgunduz/pagila

