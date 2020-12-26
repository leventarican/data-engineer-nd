import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
# log data / events
staging_events_table_create= ("""
create table if not exists staging_events(
    se_id integer IDENTITY(0,1),
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession INT,
    lastName varchar,
    length FLOAT,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration BIGINT,
    sessionId INT,
    song varchar,
    status INT,
    ts TIMESTAMP,
    userAgent varchar,
    userId varchar
);
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    ss_id integer IDENTITY(0,1), 
    num_songs INT,
    artist_id varchar,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration FLOAT,
    year INT
);
""")

# The SERIAL command in Postgres is not supported in Redshift.
# IDENTITY(seed, step) --> IDENTITY(0,1) 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int identity(0,1), 
    start_time timestamp, 
    user_id int, 
    level varchar,
    song_id int, 
    artist_id int, 
    session_id int, 
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int identity(0,1),  
    first_name varchar, last_name varchar, gender varchar, level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id int identity(0,1), 
    title varchar, artist_id varchar, year int, duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id int identity(0,1), 
    name varchar, location varchar, latitude FLOAT, longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp, 
    hour int, day int, week int, month int, year int, weekday int
);
""")

# STAGING TABLES
# load (ingest) partitioned data with COPY command

# load with JSONPath file
staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}' 
region 'us-west-2' 
timeformat as 'epochmillisecs'
json {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

# load with auto option
staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}' 
region 'us-west-2'
json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
insert into users (first_name, last_name, gender, "level") (
	select distinct 
	se.firstname first_name, 
	se.lastname last_name, 
	se.gender, 
	se."level" 
	from public.staging_events se
);
""")

song_table_insert = ("""
insert into songs (title, artist_id, "year", duration) (
	select distinct 
	ss.title, 
	ss.artist_id, 
	ss."year", 
	ss.duration 
	from public.staging_songs ss
);
""")

artist_table_insert = ("""
insert into artists ("name", location, latitude, longitude) (
	select distinct 
	ss.artist_name, 
	ss.artist_location, 
	ss.artist_latitude, 
	ss.artist_longitude 
	from public.staging_songs ss
);
""")

time_table_insert = ("""
insert into "time" (
	select 
	se.ts start_time, 
	extract (hour from start_time) "hour", 
	extract (day from start_time) "day", 
	extract (week from start_time) "week",
	extract (month from start_time) "month",
	extract (year from start_time) "year", 
	extract (dayofweek from start_time) "weekday"
	from public.staging_events se
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
