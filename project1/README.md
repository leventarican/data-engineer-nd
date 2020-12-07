# project 1

## Project Overview
a basic description of the project. \
we have two type JSON sources: song data and log data. \
this project is to bring the JSON sources into a relational database (= etl).

## Project Repository files
* `create-tables.py`: create the database, drop previous tables and create tables
* ``

## How To Run the Project

## database and analytics goals
we created a relational database (postgres) in order to provide an easy way to query the data. 

## database schema design
* we have one fact table: _songplays_. rest of the tables are dimension tables
* the design relies on the star schema. the fact tables has references (foreign keys) to dimension tables. by doing this we have relations to the tables.

## ETL pipeline
to bring our data (JSON files) to database we do ETL (extract, transform and load): \
* extract: load the data from songs and log data (JSON files)
* transform: distribute data to specific tables with dataframe, time transformation, ...
* load: insert into database

## out of scope
__notes__
* JSON songs meta data resides under `songs_data/`
* JSON user activity (on app) data resides under `log_data/`

__definitions__
* _relational schema_: a logical definition of a table. like a blueprint.
* _database schema_: collection of the relational schemas for the hole database
