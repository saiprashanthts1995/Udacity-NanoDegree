import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    This is an UDF to create the spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This is an UDF to create the process song_Data file
    Load the songs and artist table 
    Load the song and artist data back again to s3 after partitioning
    '''

    # get filepath to song data file
    song_data = input_data + "song_data/"
    # song_data = input_data + "/TRABBAM128F429D223.json"

    # read song data file
    df = spark.read.json(song_data)

    # Create an Spark view to query it
    df.createOrReplaceTempView('Song_Data')

    # extract columns to create songs table
    songs_table = spark.sql(''' 
     select
        distinct cast(song_id as string) as song_id,
        cast(title as string) as title,
        cast(Artist_id as string) as artist_id,
        cast(year as int) as year,
        cast(duration as float) as duration
    from
        Song_Data
    where
        song_id is not null
     ''')

    # Create an Spark view to query it
    songs_table.createOrReplaceTempView('songs_table')

    # print(songs_table.show(1))

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + '/Song_Table',mode='overwrite')

    # extract columns to create artists table
    artists_table = spark.sql('''
     select
        distinct cast(artist_id as string) as artist_id,
        cast(artist_name as string) as artist_name,
        cast(artist_location as string) as artist_location,
        cast(artist_latitude as float) as artist_latitude,
        cast(artist_longitude as float) as artist_longitude
    from
        Song_Data
    where
        artist_id is not null
     ''')

    # Create an Spark view to query it
    artists_table.createOrReplaceTempView('artists_table')

    # print(artists_table.show(1))

    # write artists table to parquet files
    artists_table.write.parquet(output_data + '/artists_table',mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    This is an UDF to create the process log_data file
    By filtering the data based on Page = "Next Song"
    Load the user and time table
    Load the user and time data back again to s3 after partitioning
    Load the songs_play data using the 2 provided files
    '''

    # get filepath to log data file
    log_data = input_data + "log_data/"
    # log_data = input_data + "2018-11-07-events.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['Page'] == 'NextSong')

    df.createOrReplaceTempView('log_data')

    # extract columns for users table
    users_table = spark.sql('''
     select
        distinct cast(userid as int) as user_id,
        cast(firstname as string) as first_name,
        cast(lastname as string) as last_name,
        cast(gender as string) as gender,
        cast(level as string) as level
    from
        log_data
    where
        userid is not null
     ''')

    # print(users_table.show())

    # write users table to parquet files
    users_table.write.parquet(output_data + '/users_table',mode='overwrite')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df =

    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df =

    # Above stuff is not required as the above utility can be achieved using spark sql

    # extract columns to create time table
    time_table = spark.sql(''' 
     select
        distinct derived.starttime as start_time,
        hour(derived.starttime) as hour,
        dayofmonth(derived.starttime) as day,
        weekofyear(derived.starttime) as week,
        month(derived.starttime) as month,
        year(derived.starttime) as year,
        dayofweek(derived.starttime) as weekday
    from
        (
        select
            cast((ts / 1000) as timestamp) as starttime
        from
            log_data
        where
            ts is not null )derived
     ''')

    # print(time_table.show())

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + '/time_table',mode='overwrite')

    #
    # song_data = input_data + "/TRABBAM128F429D223.json"
    #
    # spark.read.json(song_data).show(1)
    #
    # # read song data file
    # df = spark.read.json(song_data)
    #
    # df.createOrReplaceTempView('Song_Data')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(''' 
    SELECT
        DISTINCT monotonically_increasing_id() as songplay_id,
        cast((SE.ts / 1000) as timestamp) as start_time,
        month(cast((SE.ts / 1000) as timestamp)) as month,
        year(cast((SE.ts / 1000) as timestamp)) as year,
        cast(SE.USERID as int) as USER_ID,
        cast(SE.LEVEL as string) as USER_LEVEL,
        cast(SS.song_id as string) as song_id ,
        cast(SS.artist_id as string) as artist_id ,
        cast(SE.sessionid as int) as session_id ,
        cast(SE.location as string) as location ,
        cast(SE.useragent as string) as useragent
    FROM
        log_data SE
    JOIN Song_Data SS ON
        SE.artist = SS.artist_id
    WHERE
        UPPER(SE.page) = 'NEXTSONG'
    ''')

    # print(songplays_table.show())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data + '/songplays_table',mode='overwrite')


def main():
    '''
    This is an UDF to call above functions
    '''
    spark = create_spark_session()
    # input_data = "data/"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/Sai/"
    # output_data = ""
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
