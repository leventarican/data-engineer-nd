import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# debug
from pyspark.sql.types import *
#from pyspark.sql.functions import lit
#from pyspark.sql.functions import from_unixtime as from_unixtime_spark
#from pyspark.sql import functions as F

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    example data
    https://s3.console.aws.amazon.com/s3/object/udacity-dend?region=us-west-2&prefix=song-data/A/A/A/TRAAAAK128F9318786.json
    
    1. extract data
    2. transform data
    3. load data
    '''
    
    # get filepath to song data file
    # song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    
    # s3://udacity-dend/song-data/A/A/A/TRAAAAK128F9318786.json
    song_data = input_data + "song-data/A/A/A/TRAAAAK128F9318786.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
#    print(df)
#    DataFrame[
#        artist_id: string, artist_latitude: string, artist_location: 
#        string, artist_longitude: string, artist_name: string, 
#        duration: double, num_songs: bigint, song_id: string, title: string, year: bigint]

    # #########################################################################
    # songs - songs in music database
    # song_id, title, artist_id, year, duration
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    #df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # print(songs_table)
    # DataFrame[song_id: string, title: string, artist_id: string, year: bigint, duration: double] 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data, "songs"))
    
    # #########################################################################
    # artists - artists in music database
    # artist_id, name, location, lattitude, longitude
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists"))

def process_log_data(spark, input_data, output_data):
    '''
    example data
    s3://udacity-dend/log-data/2018/11/2018-11-01-events.json
    
    https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
    '''
    
    # get filepath to log data file
    log_data = input_data + "log-data/2018/11/2018-11-01-events.json"

    # read log data file
    df = spark.read.json(log_data)
    
#    print(df)
#    DataFrame[artist: string, auth: string, firstName: string, gender: string, 
#              itemInSession: bigint, lastName: string, length: double, level: string, 
#              location: string, method: string, page: string, registration: double, 
#              sessionId: bigint, song: string, status: bigint, ts: bigint, 
#              userAgent: string, userId: string]
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # #########################################################################
    # users - users in the app
    # user_id, first_name, last_name, gender, level
    
    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.show()
    # +------+---------+--------+------+-----+                                                                    
    # |userId|firstName|lastName|gender|level|
    # +------+---------+--------+------+-----+
    # |     8|   Kaylee| Summers|     F| free|
    # |    10|   Sylvie|    Cruz|     F| free|
    # |    26|     Ryan|   Smith|     M| free|
    # |   101|   Jayden|     Fox|     M| free|
    # +------+---------+--------+------+-----+
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))
    
    # #########################################################################
    # time - timestamps of records in songplays broken down into specific units
    # start_time, hour, day, week, month, year, weekday

    df.select("ts").show()
#    +-------------+                                                                                                         
#    |           ts|
#    +-------------+
#    |1541106106796|
#    |1541106352796|

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(int(ts)/1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    df.select(df.timestamp).show()
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(int(ts)/1000.0), DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    df.select("timestamp", "datetime").show()
    #df.select("start_time").show()
    
    typ = df.schema["timestamp"].dataType
    print(typ)
    typ = df.schema["datetime"].dataType
    print(typ)
    
    # extract columns to create time table
    time_table = df.select("ts", "timestamp", "datetime", \
                           hour(df.timestamp).alias("hour"), \
                           dayofmonth(df.timestamp).alias("day"), \
                           weekofyear(df.datetime).alias("week"), \
                           month(df.datetime).alias("month"), \
                           year(df.datetime).alias("year"), \
                           date_format(df.timestamp, "E").alias("weekday")\
                          ).dropDuplicates()
    time_table.show()
# +-------------+--------------------+----------+----+---+----+-----+----+-------+                                                              
# |           ts|           timestamp|  datetime|hour|day|week|month|year|weekday|
# +-------------+--------------------+----------+----+---+----+-----+----+-------+
# |1541107493796|2018-11-01 21:24:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541106352796|2018-11-01 21:05:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541107053796|2018-11-01 21:17:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541106673796|2018-11-01 21:11:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541106496796|2018-11-01 21:08:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541109325796|2018-11-01 21:55:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541109125796|2018-11-01 21:52:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541107734796|2018-11-01 21:28:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541106106796|2018-11-01 21:01:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# |1541110994796|2018-11-01 22:23:...|2018-11-01|  22|  1|  44|   11|2018|    Thu|
# |1541108520796|2018-11-01 21:42:...|2018-11-01|  21|  1|  44|   11|2018|    Thu|
# +-------------+--------------------+----------+----+---+----+-----+----+-------+
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "time"))

    # #########################################################################
    # join log and song data tables
    # songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song-data/A/A/A/TRAAAAK128F9318786.json")
    song_df.createOrReplaceTempView("song_data")

    songplays_table = spark.sql("""
    select * from song_data limit 3
    """);
    print(type(songplays_table))
    songplays_table.show()
#+------------------+---------------+---------------+----------------+------------+--------+---------+------------------+------+----+
# |         artist_id|artist_latitude|artist_location|artist_longitude| artist_name|duration|num_songs|           song_id| title|year|
# +------------------+---------------+---------------+----------------+------------+--------+---------+------------------+------+----+
# |ARJNIUY12298900C91|           null|               |            null|Adelitas Way|213.9424|        1|SOBLFFE12AF72AA5BA|Scream|2009|
# +------------------+---------------+---------------+----------------+------------+--------+---------+------------------+------+----+
    
    df.limit(3).show()
# +----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+--------------------+----------+
# |    artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page|     registration|sessionId|                song|status|           ts|           userAgent|userId|           timestamp|  datetime|
# +----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+--------------------+----------+
# |   Des'ree|Logged In|   Kaylee|     F|            1| Summers|246.30812| free|Phoenix-Mesa-Scot...|   PUT|NextSong|1.540344794796E12|      139|        You Gotta Be|   200|1541106106796|"Mozilla/5.0 (Win...|     8|2018-11-01 21:01:...|2018-11-01|
    
    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("log_data")
    songplays_table = spark.sql("""
    SELECT DISTINCT 
        row_number() OVER (PARTITION BY sd.song_id ORDER BY ld.userId DESC) as songplay_id,
        ts as start_time, month(timestamp) as month, year(timestamp) as year,
        ld.userId as user_id, ld.level, sd.song_id, sd.artist_id,
        ld.sessionId as session_id, ld.location, ld.userAgent as user_agent
    FROM log_data ld
    JOIN song_data sd ON 
        ld.artist = sd.artist_name
        and ld.song = sd.title
        and ld.length = sd.duration
    """)

    songplays_table.show()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "songplays"))

def debug(spark):
    '''
    just for debugging 
    
    https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/types/StructType.html
    '''
    
    # java.lang.String name, java.lang.String dataType, boolean nullable
    schema = StructType([StructField('A', StringType(), True)])
    df = spark.createDataFrame([("a",), ("b",), ("c",)], schema)
    df.show()
    
    schema1 = StructType(
        [
            StructField('id', StringType(), True),
            StructField('ts', StringType(), True)
        ])
    data1 = [
        ("a", "b"),
        ("001", "2010-01-01")
    ]
    df1 = spark.createDataFrame(data=data1, schema=schema1)
    df1.withColumn("timestamp", F.col("ts")).show()
    
#    +---+----------+----------+
#    | id|        ts| timestamp|
#    +---+----------+----------+
#    |  a|         b|         b|
#    |001|2010-01-01|2010-01-01|
#    +---+----------+----------+

    df1.withColumn("timestamp", F.col("ts")).withColumn("foo", lit("bar")).show()
    
#    +---+----------+----------+---+
#    | id|        ts| timestamp|foo|
#    +---+----------+----------+---+
#    |  a|         b|         b|bar|
#    |001|2010-01-01|2010-01-01|bar|
#    +---+----------+----------+---+

    udf_developer = udf(lambda exp: "senior" if exp >=10 else "junior", StringType())
    df2 = spark.createDataFrame([{"programming_language": "java", "exp": 20}])
    df2.withColumn("senior/junior dev", udf_developer(df2.exp)).show()
    
#    +---+--------------------+-----------------+
#    |exp|programming_language|senior/junior dev|
#    +---+--------------------+-----------------+
#    | 20|                java|           senior|
#    +---+--------------------+-----------------+
    
def main():

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "."
    
    #process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    
    #debug(spark)

if __name__ == "__main__":
    main()
