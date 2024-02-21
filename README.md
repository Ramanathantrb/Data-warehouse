# Project Data Warehouse
## Project Overview

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project aims to build an ETL pipeline for a music streaming startup, Sparkify, to move their data from S3 to a data warehouse hosted on Redshift. The data consists of JSON logs on user activity and JSON metadata on songs. The goal is to create a database schema and implement an ETL process to extract, transform, and load the data into the data warehouse.

## Song Dataset 
We will be working with two datasets that reside in S3. 

#### Song Dataset: 
It's a subset of real data from [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Sample Data:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Log Dataset
The second dataset consists of log files in JSON format generated by this  [event simulator](https://github.com/Interana/eventsim)  based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month. 

Sample Data: 

    {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}


## Schema for Song Play Analysis
The database schema follows a star schema design, consisting of one fact table and four dimension tables. The fact table, songplay_table, records song play events, while the dimension tables (user_table, song_table, artist_table, time_table) provide additional details about users, songs, artists, and timestamps.
#### Fact Table
songplay_table - records in event data associated with song plays. Columns for the table:

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables 
##### user_table

    user_id, first_name, last_name, gender, level
##### song_table

    song_id, title, artist_id, year, duration

##### artist_table

    artist_id, name, location, lattitude, longitude

##### time_table

    start_time, hour, day, week, month, year, weekday
The chosen schema for this project is a star schema. A star schema is a simple and widely-used data warehouse schema design that consists of a fact table surrounded by dimension tables.

In this case, the fact table is the songplays table, which records information about the songs played by users. It contains foreign keys to the dimension tables and captures the details of each song play event.

## Project Structure
The project repository contains the following files:

    create_tables.py: This script connects to the Redshift database, drops any existing tables, and creates new tables based on the SQL statements defined in sql_queries.py.
    etl.py: This script connects to the Redshift database, loads data from S3 into staging tables, and then transforms and loads the data into the analytics tables.
    sql_queries.py: This file contains all the SQL queries used in the project, including the CREATE, DROP, and INSERT statements for the tables.
    dwh.cfg: This configuration file contains the necessary settings for connecting to the Redshift cluster and accessing the input data on S3.

## How to run the scripts


> 1.Set up the necessary configurations in the 'dwh.cfg' file. Provide your AWS and Redshift credentials, as well as the S3 file paths for the input data.

> 2.Run the 'create_tables.py' script to create the necessary tables in the Redshift database. This script will drop any existing tables and create new ones based on the SQL statements defined in sql_queries.py.

> 3.Once the tables are created, run the etl.py script to load data from S3 into the staging tables and then transform and load the data into the analytics tables.

After running the ETL process, you can perform analytic queries on the Redshift database to analyze the data.