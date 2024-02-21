import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DB_NAME='dev'
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439
dwh_region ='us-east-1'
dwh_role_arn = ''
dwh_endpoint=''
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
    )
"""
)



staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_location VARCHAR,
    artist_longitude FLOAT,
    artist_name VARCHAR,
    duration FLOAT,
    num_songs INT,
    song_id VARCHAR,
    title VARCHAR,
    year INT
    )
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
    (
    songplay_id INT IDENTITY(0,1) primary key sortkey,
    start_time timestamp distkey,
    user_id int ,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
    )
""")
user_table_create = (""" CREATE TABLE IF NOT EXISTS user_table(
    user_id int PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
    (
    song_id varchar PRIMARY KEY sortkey,
    title varchar, 
    artist_id varchar,
    year int,
    duration numeric
    )
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
    (
    artist_id varchar PRIMARY KEY sortkey,
    name varchar, 
    location varchar, 
    latitude numeric,
    longitude numeric
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table
    (
    start_time timestamp PRIMARY KEY sortkey distkey,
    hour int, 
    day int,
    week int,
    month int,
    year int,
    weekday int
    )
""")



# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events_table
    FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json '{}'
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,dwh_role_arn,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs_table from 's3://udacity-dend/song_data'

iam_role 'arn:aws:iam::591339654753:role/myRedshiftRole'
region 'us-west-2'
format as json 'auto';

""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT 
    a.ts ,
    a.userId ,
    a.level ,
    b.song_id ,
    b.artist_id ,
    a.sessionId ,
    a.location ,
    a.userAgent 
    FROM staging_events_table a
        LEFT JOIN staging_songs_table b
        ON a.song = b.title AND a.artist = b.artist_name and a.length = b.duration
    WHERE a.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    (SELECT DISTINCT 
    userId ,
    firstName ,
    lastName ,
    gender,
    level
    FROM staging_events_table
    WHERE userId IS NOT NULL
    AND page = 'NextSong')
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
    (SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs_table
    WHERE song_id IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    (SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs_table
    WHERE artist_id IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    (SELECT DISTINCT 
    ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(dayofweek FROM ts)
    FROM staging_events_table
    WHERE ts IS NOT NULL)
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
