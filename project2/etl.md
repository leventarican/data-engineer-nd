
# Github Repository
* https://github.com/leventarican/data-engineer-nd - Project 2

# Part I. ETL Pipeline for Pre-Processing the Files

## Import Python packages 


```python
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

## Creating list of filepaths to process original event csv data files


```python
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print(file_path_list)
```

    /home/workspace
    ['/home/workspace/event_data/2018-11-27-events.csv', '/home/workspace/event_data/2018-11-04-events.csv', '/home/workspace/event_data/2018-11-07-events.csv', '/home/workspace/event_data/2018-11-09-events.csv', '/home/workspace/event_data/2018-11-19-events.csv', '/home/workspace/event_data/2018-11-05-events.csv', '/home/workspace/event_data/2018-11-22-events.csv', '/home/workspace/event_data/2018-11-16-events.csv', '/home/workspace/event_data/2018-11-26-events.csv', '/home/workspace/event_data/2018-11-24-events.csv', '/home/workspace/event_data/2018-11-29-events.csv', '/home/workspace/event_data/2018-11-15-events.csv', '/home/workspace/event_data/2018-11-20-events.csv', '/home/workspace/event_data/2018-11-06-events.csv', '/home/workspace/event_data/2018-11-18-events.csv', '/home/workspace/event_data/2018-11-21-events.csv', '/home/workspace/event_data/2018-11-10-events.csv', '/home/workspace/event_data/2018-11-23-events.csv', '/home/workspace/event_data/2018-11-02-events.csv', '/home/workspace/event_data/2018-11-28-events.csv', '/home/workspace/event_data/2018-11-03-events.csv', '/home/workspace/event_data/2018-11-13-events.csv', '/home/workspace/event_data/2018-11-30-events.csv', '/home/workspace/event_data/2018-11-12-events.csv', '/home/workspace/event_data/2018-11-01-events.csv', '/home/workspace/event_data/2018-11-14-events.csv', '/home/workspace/event_data/2018-11-25-events.csv', '/home/workspace/event_data/2018-11-08-events.csv', '/home/workspace/event_data/2018-11-17-events.csv', '/home/workspace/event_data/2018-11-11-events.csv']


## Create CSV file: 
* Processing the files to create the data file csv that will be used for Apache Casssandra tables


```python
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

print("composite CSV file created.")
```

    composite CSV file created.


## Check the number of rows in your csv file


```python
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821


### Read as _pandas dataframe_, show type and display some data


```python
data_csv = 'event_datafile_new.csv'
df = pd.read_csv(data_csv)
```


```python
df.dtypes
```




    artist            object
    firstName         object
    gender            object
    itemInSession      int64
    lastName          object
    length           float64
    level             object
    location          object
    sessionId          int64
    song              object
    userId             int64
    dtype: object




```python
#df = df[df["itemInSession"] == 4]
#df = df[df["sessionId"] == 338]
#df.head(10)

#df = df[df["userId"] == 10]
#df = df[df["sessionId"] == 182]
#df.head(10)

df = df[df["song"] == "All Hands Against His Own"]
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>sessionId</th>
      <th>song</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2792</th>
      <td>The Black Keys</td>
      <td>Tegan</td>
      <td>F</td>
      <td>25</td>
      <td>Levine</td>
      <td>196.91057</td>
      <td>paid</td>
      <td>Portland-South Portland, ME</td>
      <td>611</td>
      <td>All Hands Against His Own</td>
      <td>80</td>
    </tr>
    <tr>
      <th>5135</th>
      <td>The Black Keys</td>
      <td>Sara</td>
      <td>F</td>
      <td>31</td>
      <td>Johnson</td>
      <td>196.91057</td>
      <td>paid</td>
      <td>Winston-Salem, NC</td>
      <td>152</td>
      <td>All Hands Against His Own</td>
      <td>95</td>
    </tr>
    <tr>
      <th>6298</th>
      <td>The Black Keys</td>
      <td>Jacqueline</td>
      <td>F</td>
      <td>50</td>
      <td>Lynch</td>
      <td>196.91057</td>
      <td>paid</td>
      <td>Atlanta-Sandy Springs-Roswell, GA</td>
      <td>559</td>
      <td>All Hands Against His Own</td>
      <td>29</td>
    </tr>
  </tbody>
</table>
</div>



# Part II. Complete the Apache Cassandra coding portion of your project. 

Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns:
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">

## Begin writing your Apache Cassandra code in the cells below

### Creating a Cluster


```python
# This should make a connection to a Cassandra instance your local machine (127.0.0.1)
# To establish connection and begin executing queries, need a session

from cassandra.cluster import Cluster

try: 
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)
```

### Create Keyspace


```python
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

### Set Keyspace


```python
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)
```

### Now we need to create tables
* Remember, with Apache Cassandra you model the database tables on the queries you want to run.

__Create queries to ask the following three questions of the data__
1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### Create table
* iteminsession = 4, sessionid = 338
* iteminsession and sessionid are considered as composite partition key because they are used in query 1 for the WHERE clause.


```python
query = "CREATE TABLE IF NOT EXISTS artist_songs "
query = query + "(iteminsession int, sessionid int, length float, song text, artist text, \
PRIMARY KEY ((iteminsession, sessionid)) )"
try:
    session.execute(query)
except Exception as e:
    print(e)
```

### Create table
* with userid and sessionid = 182, userid = 10
* userid and sessionid are considered as composite partition key because they are used in query 1 for the WHERE clause. Additionaly we are using here a clustering key iteminsession for sorting purposes.


```python
query = "CREATE TABLE IF NOT EXISTS song_playlist_session "
query = query + "(userid int, sessionid int, iteminsession int, \
artist text, firstname text, lastname text, song text, length float, \
PRIMARY KEY ((userid, sessionid), iteminsession) )"
try:
    session.execute(query)
except Exception as e:
    print(e)
```

### Create table
* song = 'All Hands Against His Own'
* song is considered as partition key and userid as clustering key. Because we are searching for a song depending of the user.


```python
query = "CREATE TABLE IF NOT EXISTS song_users "
query = query + "(song text, userid int, firstname text, lastname text, \
PRIMARY KEY (song, userid) )"
try:
    session.execute(query)
except Exception as e:
    print(e)
```

### Insert / Distribute data to tables


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO artist_songs (iteminsession, sessionid, length, artist, song)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (
            int(line[3]), int(line[8]), float(line[5]), line[0], line[9]
        ))
        query = "INSERT INTO song_playlist_session (userid, sessionid, iteminsession, artist, firstname, lastname, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (
            int(line[10]), int(line[8]), int(line[3]), line[0], line[1], line[4], line[9], float(line[5])
        ))
        query = "INSERT INTO song_users (song, userid, firstname, lastname)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (
            line[9], int(line[10]), line[1], line[4]
        ))
print("insert data to tables done.")
```

    insert data to tables done.


### Query 1:
* Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4


```python
query = "select artist, song, length from artist_songs where iteminsession = 4 and sessionid = 338"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.artist, row.song, row.length)
except Exception as e:
    print(e)  
```

    Faithless Music Matters (Mark Knight Dub) 495.30731201171875


### Query 2: 
* Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182


```python
query = "select artist, song, firstname, lastname, iteminsession from song_playlist_session \
where userid = 10 and sessionid = 182"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.artist, row.song, row.firstname, row.lastname, row.iteminsession)
except Exception as e:
    print(e)   
```

    Down To The Bone Keep On Keepin' On Sylvie Cruz 0
    Three Drives Greece 2000 Sylvie Cruz 1
    Sebastien Tellier Kilometer Sylvie Cruz 2
    Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Sylvie Cruz 3


### Query 3: 
* Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own' query = "select firstname, lastname from music_app_history limit 2"


```python
query = "select firstname, lastname from song_users where song = 'All Hands Against His Own'"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.firstname, row.lastname)
except Exception as e:
    print(e)
```

    Jacqueline Lynch
    Tegan Levine
    Sara Johnson


### Drop the tables before closing out the sessions


```python
try:
    session.execute("drop table artist_songs")
    session.execute("drop table song_playlist_session")
    session.execute("drop table song_users")
except Exception as e:
    print(e)
```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```


```python

```
