# Sparkify DB WHS

## Overview
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I was tasked with building an ETL pipeline that extracts their data from Amazon Web Services (AWS) S3 data storage buckets (songs/artists json and user activity json), stages them in AWS Redshift (columnar data storage), and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Schema for Song Play Analysis

### Fact Table

`songplays`: records in event data associated with song plays i.e. records with page NextSong.

field         | type              |   
--------------|-------------------|---
songplay_id   | INT IDENTITY(0,1) | PRIMARY KEY
start_time    | TIMESTAMP         | FOREIGN KEY
user_id       | INTEGER           | FOREIGN KEY
song_id       | VARCHAR           | FOREIGN KEY
artist_id     | VARCHAR           | FOREIGN KEY
session_id    | BIGINT            |
user_level    | VARCHAR           |
user_location | VARCHAR           |
user_agent    | TEXT              |

NOTE: IDENTITY is the equivalent to SERIAL in Redshift.

### Dimension Tables

`users`: Sparkify users' metadata.

field       | type    |   
------------|---------|---
user_id     | INTEGER | PRIMARY KEY
first_name  | VARCHAR |
last_name   | VARCHAR |
user_gender | VARCHAR |
user_level  | VARCHAR |

`songs`: songs in music database. 

field         | type    |   
--------------|---------|---
song_id       | VARCHAR | PRIMARY KEY
song          | VARCHAR |
artist_id     | VARCHAR |
year          | INTEGER |
song_duration | FLOAT   |


`artists`: artists in music database.

field            | type    |   
-----------------|---------|---
artist_id        | VARCHAR | PRIMARY KEY
artist           | VARCHAR |
artist_location  | VARCHAR |
artist_latitude  | FLOAT   |
artist_longitude | FLOAT   |


`time`: timestamps of records in songplays broken down into specific units

field      | type      |   
-----------|-----------|---
start_time | TIMESTAMP | PRIMARY KEY
hour       | INTEGER   |
day        | INTEGER   |
week       | INTEGER   |
month      | INTEGER   |
year       | INTEGER   |
weekday    | INTEGER   |

## Process

First, to run this, a user would need to write the configuration of an AWS Cluster into `dwh.cfg` (IAM user role, Redshift). JSON files/logs are not included, they come from [Million Song](https://labrosa.ee.columbia.edu/millionsong/) dataset. Having the correct files in an S3 bucket, anyone can run `python create_table.py` and `python etl.py` to make the database functionable.

- create_table.py creates fact and dimension tables for the star schema in Redshift.
- etl.py loads data from S3 into staging tables on Redshift and then processes that data into your analytical tables on Redshift.
- sql_queries.py defines all the SQL statements, which are imported into the files above.
