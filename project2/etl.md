
# Part I. ETL Pipeline for Pre-Processing the Files

## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

#### Import Python packages 


```python
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

#### Creating list of filepaths to process original event csv data files


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


#### Processing the files to create the data file csv that will be used for Apache Casssandra tables


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

    #



```python
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821



```python
# read as pandas dataframe, show type and display 2 sample data
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

## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
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

#### Creating a Cluster


```python
# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)
# To establish connection and begin executing queries, need a session

from cassandra.cluster import Cluster

try: 
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)
```

#### Create Keyspace


```python
# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

#### Set Keyspace


```python
# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)
```

### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

## Create queries to ask the following three questions of the data

### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4


### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    

### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'





```python
# 1. Give me the artist, song title and song's length in the music app history that was heard during 
# sessionId = 338, and itemInSession = 4

# create table: partition with iteminsession and sessionid, sort order asc = length
query = "CREATE TABLE IF NOT EXISTS m0 "
query = query + "(artist text, song text, length float, \
iteminsession int, sessionid int, \
PRIMARY KEY ((iteminsession, sessionid), length))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) 
# for userid = 10, sessionid = 182

# create table: partition with userid and sessionid, sort order asc = iteminsession
query = "CREATE TABLE IF NOT EXISTS m1 "
query = query + "(artist text, \
firstname text, lastname text, userid int, \
iteminsession int, sessionid int, \
song text, length float, \
PRIMARY KEY ((userid, sessionid), iteminsession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# 3. Give me every user name (first and last) in my music app history who listened to the 
# song 'All Hands Against His Own'

# create table: partition with song, sort order asc = firstname, lastname
query = "CREATE TABLE IF NOT EXISTS m2 "
query = query + "(song text, firstname text, lastname text, \
PRIMARY KEY (song, firstname, lastname) )"
try:
    session.execute(query)
except Exception as e:
    print(e)
```


```python
# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO m0 (artist, song, length, iteminsession, sessionid)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (
            line[0], line[9], float(line[5]), int(line[3]), int(line[8])
        ))
        query = "INSERT INTO m1 (artist, firstname, lastname, userid,  iteminsession, sessionid, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (
            line[0], line[1], line[4], int(line[10]), int(line[3]), int(line[8]), line[9], float(line[5])
        ))
        query = "INSERT INTO m2 (song, firstname, lastname)"
        query = query + " VALUES (%s, %s, %s)"
        session.execute(query, (
            line[9], line[1], line[4]
        ))
print("insert data to tables done.")
```

    insert data to tables done.



```python

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
  </tbody>
</table>
</div>



#### Do a SELECT to verify that the data have been inserted into each table


```python

```


```python
## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("select * from m0 limit 2")
    for row in rows:
        print (row.artist, row.song)
    print("#")
    rows = session.execute("select * from m1 limit 2")
    for row in rows:
        print (row.artist, row.song)
    print("#")
    rows = session.execute("select * from m2 limit 100")
    for row in rows:
        print (row.song, row.firstname, row.lastname)
except Exception as e:
    print(e)
```

    Tom Petty And The Heartbreakers The Wild One_ Forever
    Kenny G with Peabo Bryson By The Time This Night Is Over
    #
    System of a Down Sad Statue
    Ghostland Observatory Stranger Lover
    #
    Wonder What's Next Chloe Cuevas
    In The Dragon's Den Chloe Cuevas
    Too Tough (1994 Digital Remaster) Aleena Kirby
    Rio De Janeiro Blue (Album Version) Chloe Cuevas
    My Place Jacob Klein
    My Place Lily Koch
    The Lucky Ones Layla Griffin
    I Want You Now Tegan Levine
    Why Worry Mohammad Rodriguez
    TvÃÂ¡rÃÂ­ v TvÃÂ¡r Kate Harrell
    Lord Chancellor's Nightmare Song Kinsley Young
    Misfit Love Jayden Graves
    Eat To Live (Amended Version) Sara Johnson
    Hey_ Soul Sister Aleena Kirby
    Hey_ Soul Sister Carlos Carter
    Hey_ Soul Sister Harper Barrett
    Hey_ Soul Sister Hayden Brock
    Hey_ Soul Sister Jacob Klein
    Hey_ Soul Sister Jacqueline Lynch
    Hey_ Soul Sister Kate Harrell
    Hey_ Soul Sister Magdalene Herman
    Hey_ Soul Sister Ryan Smith
    Hey_ Soul Sister Tegan Levine
    Hey_ Soul Sister Wyatt Scott
    You Never Let Go Avery Watkins
    I Found That Essence Rare Cecilia Owens
    My Missing Kate Harrell
    To Them These Streets Belong Chloe Cuevas
    What If Kate Harrell
    Ov Fire And The Void Emily Benson
    Engwish Bwudd Kate Harrell
    Rats In The Cellar Tegan Levine
    Take Em To Church Molly Taylor
    BareNaked Kate Harrell
    Your Rocky Spine Kevin Arellano
    The Scientist Ava Robinson
    The Scientist Jacob Klein
    The Scientist Kate Harrell
    The Scientist Sara Johnson
    Slow Dancing In A Burning Room Chloe Cuevas
    Slow Dancing In A Burning Room Harper Barrett
    The Beautiful People Kate Harrell
    The Beautiful People Rylan George
    Keep Holding On Lily Koch
    Toxic Jacob Klein
    Toxic Layla Griffin
    Little Black Sandals Jayden Graves
    Bound For The Floor Jacqueline Lynch
    VooDoo Jacob Klein
    VooDoo Kinsley Young
    Me Pregunto Jacqueline Lynch
    Me Pregunto Tegan Levine
    Ubangi Stomp Aleena Kirby
    El mismo hombre Jacob Klein
    Imma Be Layla Griffin
    Lump Lily Koch
    Fast As I Can Jacqueline Lynch
    I Write Sins Not Tragedies (Album Version) Harper Barrett
    Redline (2009 Digital Remaster) Layla Griffin
    Heart of Chambers Jayden Fox
    Hard Headed Woman Layla Griffin
    Sunday Morning Sara Johnson
    Eqypt Layla Griffin
    Close To Heaven (Album Version) Emily Benson
    Ink My Whole Body Mohammad Rodriguez
    Xehasmeni Melodia Lily Koch
    Constantly Under Surveillance Mohammad Rodriguez
    This Cowboy Song Sara Johnson
    No. 5 Jayden Graves
    Fire Power Harper Barrett
    Rain Is A Good Thing Chloe Cuevas
    True Colors Aleena Kirby
    Little Susie / Pie Jesu Tegan Levine
    Someone Else's Life Ryann Smith
    It Won't Be Like This For Long Shakira Hunt
    Popular (LP Version) Jayden Graves
    Baby_ I Love Your Way (Album Version) Tegan Levine
    Berimbau First Cry Layla Griffin
    Beauty and the Beast Rylan George
    My Boy Builds Coffins Anabelle Simpson
    Murder Babe (Album) Harper Barrett
    After An Afternoon (Eagles Ballroom Live Version) Lily Burns
    After An Afternoon (Eagles Ballroom Live Version) Mohammad Rodriguez
    Smoke Chloe Cuevas
    Days Of The Week (Album Version) Chloe Cuevas
    The Lost Song Rylan George
    Jenny Take A Ride (LP Version) Tegan Levine
    Dollhouse Tegan Levine
    Calling All Angels Lily Koch
    Froggie Went a-Courtin' Lily Koch
    Bust A Move Jacqueline Lynch
    Meeting Paris Hilton (Album) Lily Koch
    Hold You Rylan George
    Wish You Well Tegan Levine
    Ron's Victory ("Harry Potter & The Half-Blood Prince") Kate Harrell
    Watching The Detectives Jayden Graves
    Loro Layla Griffin
    Oceanside Tegan Levine
    My sweet shadow Jacqueline Lynch
    Something (Album Version) Chloe Cuevas


### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS


```python
## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
query = "select artist, song, length from m0 where iteminsession = 4 and sessionid = 338"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.artist, row.song, row.length)
except Exception as e:
    print(e)  
```

    Faithless Music Matters (Mark Knight Dub) 495.30731201171875



```python
## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "select artist, song, firstname, lastname, iteminsession from m1 \
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



```python
## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
#query = "select firstname, lastname from music_app_history limit 2"
query = "select firstname, lastname from m2 where song = 'All Hands Against His Own'"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.firstname, row.lastname)
except Exception as e:
    print(e)
```

    Jacqueline Lynch
    Sara Johnson
    Tegan Levine



```python

```

### Drop the tables before closing out the sessions


```python
## TO-DO: Drop the table before closing out the sessions
try:
    session.execute("drop table m0")
    session.execute("drop table m1")
    session.execute("drop table m2")
except Exception as e:
    print(e)
```


```python

```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```


```python

```


```python

```
