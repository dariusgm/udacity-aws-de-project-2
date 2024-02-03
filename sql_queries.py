import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE log_data (
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(255),
    length NUMERIC,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration BIGINT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(255),
    userId VARCHAR(255)
);
""")

staging_songs_table_create = ("""
CREATE TABLE song_data (
    song_id VARCHAR(255),
    num_songs INT,
    title VARCHAR(255),
    artist_name VARCHAR(255),
    artist_latitude NUMERIC(10, 5),
    year INT,
    duration NUMERIC(10, 5),
    artist_id VARCHAR(255),
    artist_longitude NUMERIC(10, 5),
    artist_location VARCHAR(255)
);

""")

songplay_table_create = ("""
CREATE TABLE fact_songplays (
    songplay_id VARCHAR(255) DISTKEY, 
    start_time DATE, 
    user_id VARCHAR(255), 
    level VARCHAR(255), 
    song_id VARCHAR(255), 
    artist_id VARCHAR(255), 
    session_id INT, 
    location VARCHAR(255), 
    user_agent VARCHAR(255)
);
""")

user_table_create = ("""
CREATE TABLE dim_users (
    user_id INT DISTKEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender CHAR(1),
    level VARCHAR(255)
);
""")

song_table_create = ("""
CREATE TABLE dim_songs (
    song_id VARCHAR(255) DISTKEY,  
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INT,
    duration NUMERIC(10, 5)
);
""")

artist_table_create = ("""
CREATE TABLE dim_artists (
    artist_id VARCHAR(255) DISTKEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude NUMERIC(10, 5),
    longitude NUMERIC(10, 5)
);
""")

time_table_create = ("""
CREATE TABLE dim_time (
    start_time DATE,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY log_data FROM '$events' iam_role '$iam' 
FORMAT JSON 'auto' 
REGION 'us-west-2';
""").format()

staging_songs_copy = ("""
COPY song_data FROM '$songs' iam_role '$iam' 
FORMAT JSON 'auto' 
REGION 'us-west-2';
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays (
    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
  (TO_CHAR(
        DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE), 'YYYY-MM-DD HH24:MI:SS'
    ) || '-' || log_data.userid || '-' || song_data.song_id)::text AS songplay_id,
  DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE) AS start_time,
  log_data.userid AS user_id,
  log_data.level AS level,
  song_data.song_id AS song_id,
  song_data.artist_id AS artist_id,
  log_data.sessionid AS session_id,
  log_data.location AS "location",
  log_data.useragent AS user_agent
FROM log_data, song_data
WHERE (song_data.title = log_data.song AND song_data.artist_name = log_data.artist);

""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level) 
SELECT 
  log_data.userId::integer AS user_id,
  log_data.firstName AS first_name,
  log_data.lastName AS last_name,
  log_data.gender AS gender,
  log_data.level AS level 
FROM log_data 
WHERE log_data.userId is not null AND log_data.page = 'NextPage';

""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, "year", duration)  
SELECT 
  song_data.song_id AS song_id,
  song_data.title AS title,
  song_data.artist_id AS artist_id,
  song_data."year" AS "year",
  song_data.duration AS duration 
FROM song_data;
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude) 
SELECT 
  song_data.artist_id AS artist_id,
  song_data.artist_name AS name, 
  song_data.artist_location AS location,
  song_data.artist_latitude AS latitude,
  song_data.artist_longitude AS longitude 
FROM song_data;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour ,day, week, month, year, weekday) 
SELECT 
  DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE) AS start_time, 
  EXTRACT(HOUR FROM DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE)) AS hour,
  EXTRACT(DAY FROM DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE)) AS day,
  EXTRACT(WEEK FROM DATEADD(SECOND, log_data.ts / 1000, '1970-01-01'::DATE)) AS week,
  EXTRACT(MONTH FROM DATEADD(SECOND, log_data.ts  / 1000, '1970-01-01'::DATE)) AS month,
  EXTRACT(YEAR FROM DATEADD(SECOND, log_data.ts  / 1000, '1970-01-01'::DATE)) AS year,
  EXTRACT(WEEKDAY FROM DATEADD(SECOND, log_data.ts  / 1000, '1970-01-01'::DATE)) AS weekday
FROM log_data;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
