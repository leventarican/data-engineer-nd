import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


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

    create song and artists table
    '''
    
    print("# extract song data")
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/TRAAAAK128F9318786.json"

    # read song data file
    df = spark.read.json(song_data)

    print("# process songs")
    
    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(
        "year", "artist_id").mode("overwrite").parquet(
        os.path.join(
            output_data, "songs"))

    print("# process artists")
    
    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude")

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(
        os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    '''
    create user, time and songplays table
    '''

    print("# extract log data")
    
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    print("# process users")
    
    # extract columns for users table
    users_table = df.select(
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level").dropDuplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(
        os.path.join(output_data, "users"))

    print("# process time")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda ts: datetime.fromtimestamp(
            int(ts) / 1000.0),
        TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda ts: datetime.fromtimestamp(
            int(ts) / 1000.0), DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select("ts", "timestamp", "datetime",
                           hour(df.timestamp).alias("hour"),
                           dayofmonth(df.timestamp).alias("day"),
                           weekofyear(df.datetime).alias("week"),
                           month(df.datetime).alias("month"),
                           year(df.datetime).alias("year"),
                           date_format(df.timestamp, "E").alias("weekday")
                           ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(
        "year", "month").mode("overwrite").parquet(
        os.path.join(
            output_data, "time"))

    print("# extract song data")
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data +
                              "song-data/A/A/A/TRAAAAK128F9318786.json")
    song_df.createOrReplaceTempView("song_data")

    print("# process songsplays")
    
    # extract columns from joined song and log datasets to create songplays
    # table
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

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        "year", "month").mode("overwrite").parquet(
        os.path.join(
            output_data, "songplays"))

    print("ETL done.")

def main():

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://la-datalake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
