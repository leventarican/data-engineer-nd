
# fact table
# ##############################################################################
# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# dimension tables
# ##############################################################################
# users - users in the app
# user_id, first_name, last_name, gender, level

# songs - songs in music database
# song_id, title, artist_id, year, duration

# artists - artists in music database
# artist_id, name, location, latitude, longitude

# time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# example song data
# {
#     "num_songs": 1,
#     "artist_id": "ARMJAGH1187FB546F3",
#     "artist_latitude": 35.14968,
#     "artist_longitude": -90.04892,
#     "artist_location": "Memphis, TN",
#     "artist_name": "The Box Tops",
#     "song_id": "SOCIWDW12A8C13D406",
#     "title": "Soul Deep",
#     "duration": 148.03546,
#     "year": 1969
# }

# example log data
# {
#     "artist": null,
#     "auth": "Logged In",
#     "firstName": "Kaylee",
#     "gender": "F",
#     "itemInSession": 2,
#     "lastName": "Summers",
#     "length": null,
#     "level": "free",
#     "location": "Phoenix-Mesa-Scottsdale, AZ",
#     "method": "GET",
#     "page": "Upgrade",
#     "registration": 1540344794796.0,
#     "sessionId": 139,
#     "song": null,
#     "status": 200,
#     "ts": 1541106132796,
#     "userAgent": "\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
#     "userId": "8"
# }



# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int, start_time varchar, user_id int, level varchar, 
    song_id int, artist_id int, session_id int, location varchar, user_agent varchar
);
""")

# users - users in the app
# user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int, first_name varchar, last_name varchar, gender varchar, level varchar
);
""")

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar, title varchar, artist_id varchar, year int, duration numeric
);
""")

# artists - artists in music database
# artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id int, name varchar, location varchar, latitude numeric, longitude numeric
);
""")

# time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time varchar, hour int, day int, week int, month int, year int, weekday int
);
""")

# INSERT RECORDS

# ['ARD7TVE1187B99BFB1',
#  nan,
#  'California - LA',
#  nan,
#  'Casual',
#  218.93179,
#  1,
#  'SOMZWCG12A8C13C480',
#  "I Didn't Mean To",
#  0]

# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]