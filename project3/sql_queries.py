import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "drop table staging_events"
staging_songs_table_drop = "drop table staging_songs"
songplay_table_drop = "drop table songplay"
user_table_drop = "drop table user"
song_table_drop = "drop table song"
artist_table_drop = "drop table artist"
time_table_drop = "drop table time"

# CREATE TABLES
# log data / events
staging_events_table_create= ("""
create table if not exists staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length varchar,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId varchar,
    song varchar,
    status varchar,
    ts varchar,
    userAgent varchar,
    userId varchar
);
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    num_songs varchar,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration varchar,
    year varchar
);
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES
# load (ingest) partitioned data with COPY command

# load with JSONPath file
staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}' 
region 'us-west-2' 
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
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
