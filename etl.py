import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config[AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Args - None
    Description - Creates spark session object to be used in the session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Args
        spark - spark session object generated in create_spark_session
        input_data - directory of input data
        output_data - directory of output data
        
    Description - This function loads song data from input directory and creates dataframes for songs and artists. 
    The function also converts dataframes into parquet objects and write them to S3 bucket specified in output data
    '''
    # get filepath to song data file
    song_data = f'{input_data}/song_data'
    
    # read song data file
    songs_df = spark.read.json(song_data)

    songs_df.createOrReplaceTempView("staging_songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM staging_songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(f'{output_data}/song')

    # extract columns to create artists table
    artists_table = spark.sql("SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs")
    
    # write artists table to parquet files
    artist_table.write.parquet(f'{output_data}/artist')


def process_log_data(spark, input_data, output_data):
    '''
    Args
        spark - spark session object generated in create_spark_session
        input_data - directory of input data
        output_data - directory of output data
        
    Description - This function loads log data from input directory and creates dataframes for user, time and songplay. 
    The function also converts dataframes into parquet objects and write them to S3 bucket specified in output data
    '''
    # get filepath to log data file
    log_data = f'{input_data}/log_data'

    # read log data file
    logs_df = spark.read.json(log_data) 
    
    logs_df.createOrReplaceTempView("staging_logs")

    # extract columns for users table    
    user_table = spark.sql("SELECT userId, firstName, lastName, gender, level FROM staging_logs")
    
    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user')
    
    spark.udf.register("get_time", lambda x: datetime.datetime.fromtimestamp(x/1000).isoformat())
    spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x/1000).hour))
    spark.udf.register("get_day", lambda x: int(datetime.datetime.fromtimestamp(x/1000).day))
    spark.udf.register("get_week", lambda x: str(datetime.datetime.fromtimestamp(x/1000).isocalendar()[1]))
    spark.udf.register("get_month", lambda x: int(datetime.datetime.fromtimestamp(x/1000).month))
    spark.udf.register("get_year", lambda x: int(datetime.datetime.fromtimestamp(x/1000).year))
    spark.udf.register("get_weekday", lambda x: str(datetime.datetime.fromtimestamp(x/1000).weekday()))
    
    # extract columns to create time table
    time_table = spark.sql("SELECT get_time(ts) as start_time, get_hour(ts) as hour, get_day(ts) as day, get_week(ts) as week, get_month(ts) as month, get_year(ts) as year, get_weekday(ts) as weekday FROM staging_logs")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f'{output_data}/time')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    songs_df.createOrReplaceTempView("staging_songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("SELECT DISTINCT S.ARTIST_ID, E.LEVEL, S.ARTIST_LOCATION, E.SESSIONID, S.SONG_ID, get_time(e.ts) as start_time, E.USERAGENT, E.USERID, S.duration FROM STAGING_logS E JOIN STAGING_SONGS S ON E.song = S.title AND E.artist = S.artist_name WHERE E.PAGE = 'NextSong'")

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.partitionBy("start_time").parquet(f'{output_data}/songplay')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
