# project 1
https://github.com/leventarican/data-engineer-nd

## Project Overview
Demo of how to injest data from JSON files, transform an load it to a relational database. There are two types of JSON: _log_ and _song_ data

![ER model](er.png)

## Project Repository files
* `create_tables.py`: create the database, drop previous tables and create tables
    * on jupyter: restart kernel before run `create_tables.py`
* `sql_queries.py`: kind of a DDL
* `etl.py`: here happens the magic: read json files, process and load (to db).

## How To Run the Project
* ensure your postgres is running or use a ready jupyter notebook
* then run `python create_tables.py`
* next run the ETL process: `python etl.py` 
* check the result with the following query (should return exactly 1 row: `songplay_is=483, song_id=SOZCTXZ12AB0182364, artist_id=AR5KOSW1187FB35FF4`)
```
SELECT * FROM songplays WHERE song_id is not null;
```

## Database and analytics goals
we created a relational database (postgres) in order to provide an easy way to query the data. 

## Database schema design
* we have one fact table: _songplays_. rest of the tables are dimension tables
* the design relies on the star schema. the fact tables has references (foreign keys) to dimension tables. by doing this we have relations to the tables.

## ETL pipeline
to bring our data (JSON files) to database we do ETL (extract, transform and load): \
* extract: load the data from songs and log data (JSON files)
* transform: distribute data to specific tables with dataframe, time transformation, ...
* load: insert into database

## Example: song dataset
```
{
    "num_songs": 1,
    "artist_id": "ARMJAGH1187FB546F3",
    "artist_latitude": 35.14968,
    "artist_longitude": -90.04892,
    "artist_location": "Memphis, TN",
    "artist_name": "The Box Tops",
    "song_id": "SOCIWDW12A8C13D406",
    "title": "Soul Deep",
    "duration": 148.03546,
    "year": 1969
}
```

## Example: log dataset
```
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Kaylee",
    "gender": "F",
    "itemInSession": 2,
    "lastName": "Summers",
    "length": null,
    "level": "free",
    "location": "Phoenix-Mesa-Scottsdale, AZ",
    "method": "GET",
    "page": "Upgrade",
    "registration": 1540344794796.0,
    "sessionId": 139,
    "song": null,
    "status": 200,
    "ts": 1541106132796,
    "userAgent": Gecko
    "userId": "8"
}
```

## Out of scope
__notes__
* JSON songs meta data resides under `songs_data/`
* JSON user activity (on app) data resides under `log_data/`

__definitions__
* _relational schema_: a logical definition of a table. like a blueprint.
* _database schema_: collection of the relational schemas for the hole database
