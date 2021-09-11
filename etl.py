import configparser
import datetime
import calendar
import os
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['IAM_USER']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['IAM_USER']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song JSON files and writes them to another S3 bucket as Parquet files.
    Parameters: Spark session, input path, output path
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =  df.select('song_id',
                             'title',
                             'artist_id',
                             'year',
                             'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet', mode ='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude') \
                            .withColumnRenamed('artist_name', 'name') \
                            .withColumnRenamed('artist_location', 'location') \
                            .withColumnRenamed('artist_latitude', 'latitude') \
                            .withColumnRenamed('artist_longitude', 'longitude') \
                            .distinct()

    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet', mode ='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes log JSON files and writes them to another S3 bucket as Parquet files.
    Parameters: Spark session, input path, output path
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId',
                            'firstName',
                            'lastName',
                            'gender',
                            'level') \
                          .withColumnRenamed('userId', 'user_id') \
                          .withColumnRenamed('firstName', 'first_name') \
                          .withColumnRenamed('lastName', 'last_name') \
                          .distinct()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet', mode='overwrite')
    
    # create start_time column from original timestamp column
    df = df.withColumn(
        'start_time',
        F.to_timestamp(F.from_unixtime((col('ts') / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast('Timestamp')
    )
     
    def get_weekday(date):
        """
        Gets day of week from date.
        Parameters: date
        Returns: day of week
        """
        date = date.strftime("%m-%d-%Y")
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType())

    # extract columns to create time table
    time_table = df.withColumn('hour', hour(col('start_time'))) \
                   .withColumn('day', dayofmonth(col('start_time'))) \
                   .withColumn('week', weekofyear(col('start_time'))) \
                   .withColumn('month', month(col('start_time'))) \
                   .withColumn('year', year(col('start_time'))) \
                   .withColumn('weekday', udf_week_day(col('start_time'))) \
                   .select('start_time',
                            'hour',
                            'day',
                            'week',
                            'month',
                            'year',
                            'weekday').distinct()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet', mode ='overwrite')
                                                                
    # read in song and artist data to use for songplays table
    songs_table = spark.read.parquet(output_data + 'songs.parquet')
    artists_table = spark.read.parquet(output_data + 'artists.parquet')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn('songplay_id', F.monotonically_increasing_id()) \
                        .join(songs_table, (songs_table.title == df.song) & (df.length == songs_table.duration)) \
                        .join(artists_table, (df.artist == artists_table.name) & (songs_table.artist_id == artists_table.artist_id)) \
                        .join(time_table, time_table.start_time == df.start_time) \
                        .select('songplay_id',
                                df['start_time'],
                                df['userId'],
                                df['level'],
                                songs_table['song_id'],
                                songs_table['artist_id'],
                                df['sessionId'],
                                artists_table['location'],
                                df['userAgent'],
                                time_table['year'],
                                time_table['month']) \
                             .withColumnRenamed('userId', 'user_id') \
                             .withColumnRenamed('sessionId', 'session_id') \
                             .withColumnRenamed('userAgent', 'user_agent') 
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet', mode='overwrite')

def main():
    spark = create_spark_session()
    
    # If running from Udacity workspace, use s3a instead of s3
    # Use s3 when running on EMR cluster
    input_data = 's3://udacity-dend/'
    output_data = 's3://data-lake-pmb/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == '__main__':
    main()
