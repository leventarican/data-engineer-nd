# project 2
https://github.com/leventarican/data-engineer-nd

* use ETL to extract our CSV dataset in Apache Cassandra

## Project Description
* Data Analysts want to know what songs the users are listening. Data resides as CSV.
* Perform data modeling with Apache Cassandra. Which can create queries on song play data.
* dataset `event_data` is partitioned by date:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## out of scope
* good approach: 1 table per 1 query
    * ex. if we have 2 queries then we need 2 different tables that partitioned the data differently
* there is no _join_ in apache cassandra. just denormalize tables (copy of data)
* a simple PRIMARY_KEY is also the PARTITION KEY. The PARTITION KEY will determine the distribution of data across the system
* _understand dataset fully_
* workflow of a basic python cassandra program
![python cassandra](python-cassandra.png)

## Links
* cassandra datatypes: https://cassandra.apache.org/doc/latest/cql/types.html
* pandas dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
