import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR, 
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT, 
    artist_id VARCHAR, 
    artist_latitude FLOAT, 
    artist_longitude FLOAT, 
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR, 
    title VARCHAR, 
    duration FLOAT, 
    year INT
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT, 
    location VARCHAR,
    user_agent VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( 
    user_id INT PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR 
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT, 
    duration FLOAT
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude FLOAT,
    longitude FLOAT
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time ( 
    start_time TIMESTAMP PRIMARY KEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday VARCHAR ENCODE BYTEDICT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
FORMAT AS json {};
""").format( config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format( config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.location,
                se.user_agent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
