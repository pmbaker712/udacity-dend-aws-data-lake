# Data Lake with S3 and Spark

A project for Udacity's Data Engineering Nanodegree.

## Project Purpose
This purpose of this project is to process song and songplay data into an AWS S3 data lake using Spark on an AWS EMR cluster.

## Datasets
The song data comes from the One Million Songs dataset, while the songplay data is simulated. Both datasets are collections of JSON files stored on AWS S3.

After creating your S3 bucket and Spark EMR cluster through the AWS Management Console or through IaC, enter your IAM credentials into `dl.cfg`:

AWS_ACCESS_KEY_ID= 
<br>
AWS_SECRET_ACCESS_KEY= 


## Create the Data Lake

This project contains one Python file, `etl.py`, which contains all of the ETL logic to create the data lake using Spark dataframes.
This file must be run on the EMR master node through an SSH tool such as PuTTY. To use SSH, make sure to allow inbound traffic on port 22 
of the EMR master node.

To run the Python file, copy `etl.py` and `dl.cfg` to the home directory of the EMR master node. This can be done through a command line text editor such as Nano.

Next, change the `output` variable in `etl.py` to your S3 bucket.

Finally, run `/usr/bin/spark-submit --master yarn ./etl.py` to submit the file as a Spark application.


## Data Lake Schema

Below are the final tables in the data lake, stored in S3 as Parquet files.
            
## Final Parquet Tables Columns

#### Songs Table (partitioned by Year and Artist):

* song_id <br>
* title <br>
* artist_id <br>
* year <br>
* duration

#### Users Table:

* song_id <br>
* title <br>
* artist_id <br>
* year <br>
* duration

#### Artists Table:

* artist_id <br>
* name  <br>
* location <br>
* latitude <br>
* longitude

#### Time Table (Partitioned by Year and Month):

* start_time <br>
* hour <br>
* day <br>
* week <br>
* month <br>
* year <br>
* weekday

#### Songplays Table (Partitioned by Year and Month):

* songplay_id <br>
* start_time <br>
* user_id <br>
* level <br>
* song_id <br>
* artist_id <br>
* session_id <br>
* location <br>
* user_agent <br>
* year <br>
* month

# Final Result Screens
<br>

### Spark History Server
<img src='Spark UI.JPG'>
<br>

### Yarn Timeline Server
<img src='Hadoop UI.JPG'>
<br>

### S3 Management Console
<img src='S3 Data Lake.JPG'>
