
# Project Title
### Data Engineering Capstone Project

#### Project Summary
--describe your project at a high level--

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


```python
# Do all imports and installs here
import pandas as pd
```

### Step 1: Scope the Project and Gather Data

#### Scope 
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

#### Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included? 

#### Dataset 1/2: vehicles


```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Data Engineer - Capstone").getOrCreate()
```


```python
df = spark.read.json("data/all-vehicles-model.json")
```


```python
df.printSchema()
```

    root
     |-- datasetid: string (nullable = true)
     |-- fields: struct (nullable = true)
     |    |-- atvtype: string (nullable = true)
     |    |-- barrels08: double (nullable = true)
     |    |-- barrelsa08: double (nullable = true)
     |    |-- c240bdscr: string (nullable = true)
     |    |-- c240dscr: string (nullable = true)
     |    |-- charge120: double (nullable = true)
     |    |-- charge240: double (nullable = true)
     |    |-- charge240b: double (nullable = true)
     |    |-- city08: long (nullable = true)
     |    |-- city08u: double (nullable = true)
     |    |-- citya08: long (nullable = true)
     |    |-- citya08u: double (nullable = true)
     |    |-- citycd: double (nullable = true)
     |    |-- citye: double (nullable = true)
     |    |-- cityuf: double (nullable = true)
     |    |-- co2: long (nullable = true)
     |    |-- co2a: long (nullable = true)
     |    |-- co2tailpipeagpm: double (nullable = true)
     |    |-- co2tailpipegpm: double (nullable = true)
     |    |-- comb08: long (nullable = true)
     |    |-- comb08u: double (nullable = true)
     |    |-- comba08: long (nullable = true)
     |    |-- comba08u: double (nullable = true)
     |    |-- combe: double (nullable = true)
     |    |-- combinedcd: double (nullable = true)
     |    |-- combineduf: double (nullable = true)
     |    |-- createdon: string (nullable = true)
     |    |-- cylinders: long (nullable = true)
     |    |-- displ: double (nullable = true)
     |    |-- drive: string (nullable = true)
     |    |-- eng_dscr: string (nullable = true)
     |    |-- engid: string (nullable = true)
     |    |-- evmotor: string (nullable = true)
     |    |-- fescore: long (nullable = true)
     |    |-- fuelcost08: long (nullable = true)
     |    |-- fuelcosta08: long (nullable = true)
     |    |-- fueltype: string (nullable = true)
     |    |-- fueltype1: string (nullable = true)
     |    |-- fueltype2: string (nullable = true)
     |    |-- ghgscore: long (nullable = true)
     |    |-- ghgscorea: long (nullable = true)
     |    |-- guzzler: string (nullable = true)
     |    |-- highway08: long (nullable = true)
     |    |-- highway08u: double (nullable = true)
     |    |-- highwaya08: long (nullable = true)
     |    |-- highwaya08u: double (nullable = true)
     |    |-- highwaycd: double (nullable = true)
     |    |-- highwaye: double (nullable = true)
     |    |-- highwayuf: double (nullable = true)
     |    |-- hlv: long (nullable = true)
     |    |-- hpv: long (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- lv2: long (nullable = true)
     |    |-- lv4: long (nullable = true)
     |    |-- make: string (nullable = true)
     |    |-- mfrcode: string (nullable = true)
     |    |-- model: string (nullable = true)
     |    |-- modifiedon: string (nullable = true)
     |    |-- mpgdata: string (nullable = true)
     |    |-- phevblended: string (nullable = true)
     |    |-- phevcity: long (nullable = true)
     |    |-- phevcomb: long (nullable = true)
     |    |-- phevhwy: long (nullable = true)
     |    |-- pv2: long (nullable = true)
     |    |-- pv4: long (nullable = true)
     |    |-- range: long (nullable = true)
     |    |-- rangea: string (nullable = true)
     |    |-- rangecity: double (nullable = true)
     |    |-- rangecitya: double (nullable = true)
     |    |-- rangehwy: double (nullable = true)
     |    |-- rangehwya: double (nullable = true)
     |    |-- scharger: string (nullable = true)
     |    |-- startstop: string (nullable = true)
     |    |-- tcharger: string (nullable = true)
     |    |-- trans_dscr: string (nullable = true)
     |    |-- trany: string (nullable = true)
     |    |-- ucity: double (nullable = true)
     |    |-- ucitya: double (nullable = true)
     |    |-- uhighway: double (nullable = true)
     |    |-- uhighwaya: double (nullable = true)
     |    |-- vclass: string (nullable = true)
     |    |-- year: string (nullable = true)
     |    |-- yousavespend: long (nullable = true)
     |-- record_timestamp: string (nullable = true)
     |-- recordid: string (nullable = true)
    



```python
df.count()
```




    41443




```python
df1 = df.select(df["fields"]["make"], df["fields"]["model"], df["fields"]["cylinders"], df["fields"]["year"])
vehicles_table = df1.toDF("brand", "model", "cylinders", "year")
vehicles_table.show(10)
```

    +----------+-------------------+---------+----+
    |     brand|              model|cylinders|year|
    +----------+-------------------+---------+----+
    |       BMW|              750il|       12|1993|
    |     Dodge|B150/B250 Wagon 2WD|        8|1985|
    |  Chrysler|         New Yorker|        6|1993|
    |     Mazda|                929|        6|1993|
    |   Pontiac|         Grand Prix|        6|1993|
    |     Volvo|                850|        5|1993|
    |     Buick|      Century Wagon|        6|1993|
    |Mitsubishi|               Expo|        4|1993|
    |Volkswagen|       Passat Wagon|        6|1993|
    |       GMC|         Sonoma 2WD|        6|1993|
    +----------+-------------------+---------+----+
    only showing top 10 rows
    



```python
vehicles_table.printSchema()
```

    root
     |-- brand: string (nullable = true)
     |-- model: string (nullable = true)
     |-- cylinders: long (nullable = true)
     |-- year: string (nullable = true)
    



```python
import os
vehicles_table.write.partitionBy("year").mode("overwrite").parquet(os.path.join("data", "vehicles"))
```

#### Dataset 2/2: news


```python
# pandas
#news = "data/abcnews-date-text.csv
#df = pd.read_csv(news, sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
#df.head()
```


```python
#df = spark.read.csv("data/abcnews-date-text.csv", header=True, inferSchema=True, mode="DROPMALFORMED", sep=",")
df = spark.read.csv("data/abcnews-date-text.csv", header=True, mode="DROPMALFORMED", sep=",")
```


```python
df.count()
```




    1082168




```python
df.printSchema()
```

    root
     |-- publish_date: string (nullable = true)
     |-- headline_text: string (nullable = true)
    



```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

get_year = udf(lambda d: d[0:4], StringType())
news_table = df.withColumn("year", get_year(df["publish_date"]))
news_table.show(10)
```

    +------------+--------------------+----+
    |publish_date|       headline_text|year|
    +------------+--------------------+----+
    |    20030219|aba decides again...|2003|
    |    20030219|act fire witnesse...|2003|
    |    20030219|a g calls for inf...|2003|
    |    20030219|air nz staff in a...|2003|
    |    20030219|air nz strike to ...|2003|
    |    20030219|ambitious olsson ...|2003|
    |    20030219|antic delighted w...|2003|
    |    20030219|aussie qualifier ...|2003|
    |    20030219|aust addresses un...|2003|
    |    20030219|australia is lock...|2003|
    +------------+--------------------+----+
    only showing top 10 rows
    



```python
news_table.printSchema()
```

    root
     |-- publish_date: string (nullable = true)
     |-- headline_text: string (nullable = true)
     |-- year: string (nullable = true)
    



```python
news_table.write.partitionBy("year").mode("overwrite").parquet(os.path.join("data", "news"))
```

### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data


```python
vehicles_table.count()
```




    41443




```python
news_table.count()
```




    1082168




```python
vehicles_table = vehicles_table.distinct()
vehicles_table.count()
```




    22836




```python
news_table = news_table.distinct()
news_table.count()
```




    1082168




```python
# Performing cleaning tasks here




```

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.


```python
df = spark.read.parquet("data/vehicles")
```


```python
df.count()
```




    41443




```python
df.createOrReplaceTempView("vehicles")
vehicles = spark.sql("""
select * from vehicles where int(year) > 2003
order by int(year) asc
""")
vehicles.show(10)
```

    +-------------+--------------------+---------+----+
    |        brand|               model|cylinders|year|
    +-------------+--------------------+---------+----+
    |      Ferrari|360 Modena/Spider...|        8|2004|
    |        Dodge|             Stratus|        4|2004|
    |        Honda|             Insight|        3|2004|
    |Mercedes-Benz|            SL55 AMG|        8|2004|
    |       Nissan|       350z Roadster|        6|2004|
    |          BMW|   330ci Convertible|        6|2004|
    |          BMW|   330ci Convertible|        6|2004|
    |        Acura|                 RSX|        4|2004|
    |       Subaru|         Impreza AWD|        4|2004|
    |       Toyota|              Celica|        4|2004|
    +-------------+--------------------+---------+----+
    only showing top 10 rows
    



```python
df = spark.read.parquet("data/news")
```


```python
df.createOrReplaceTempView("news")
news = spark.sql("""
select * from news where int(year) > 2003
order by int(year) asc
""")
news.show(10)
```


```python
vehicle_news = spark.sql("""
select v.*, n.* from vehicles v
join news n 
on v.year = n.year
""")
# where upper(n.headline_text) like upper(v.brand)
vehicle_news.show(10)
```


    ---------------------------------------------------------------------------

    KeyboardInterrupt                         Traceback (most recent call last)

    <ipython-input-82-628420fcb627> in <module>()
          6 """)
          7 # where upper(n.headline_text) like upper(v.brand)
    ----> 8 vehicle_news.show(10)
    

    /opt/spark-2.4.3-bin-hadoop2.7/python/pyspark/sql/dataframe.py in show(self, n, truncate, vertical)
        376         """
        377         if isinstance(truncate, bool) and truncate:
    --> 378             print(self._jdf.showString(n, 20, vertical))
        379         else:
        380             print(self._jdf.showString(n, int(truncate), vertical))


    /opt/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in __call__(self, *args)
       1253             proto.END_COMMAND_PART
       1254 
    -> 1255         answer = self.gateway_client.send_command(command)
       1256         return_value = get_return_value(
       1257             answer, self.gateway_client, self.target_id, self.name)


    /opt/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in send_command(self, command, retry, binary)
        983         connection = self._get_connection()
        984         try:
    --> 985             response = connection.send_command(command)
        986             if binary:
        987                 return response, self._create_connection_guard(connection)


    /opt/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in send_command(self, command)
       1150 
       1151         try:
    -> 1152             answer = smart_decode(self.stream.readline()[:-1])
       1153             logger.debug("Answer received: {0}".format(answer))
       1154             if answer.startswith(proto.RETURN_MESSAGE):


    /opt/conda/lib/python3.6/socket.py in readinto(self, b)
        584         while True:
        585             try:
    --> 586                 return self._sock.recv_into(b)
        587             except timeout:
        588                 self._timeout_occurred = True


    KeyboardInterrupt: 



```python

```

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks


```python
# Perform quality checks here
```

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.


```python

```


```python

```


```python

```


```python

```


```python

```


```python

```
