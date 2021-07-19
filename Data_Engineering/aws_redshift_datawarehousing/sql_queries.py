import configparser

# CONFIG VARIABLES
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# 2018-11-01-events.json file looks like : 
#{
#    "artist":        "Barry Tuckwell\/Academy of St Martin-in-the-Fields\/Sir Neville Marriner",
#    "auth":          "Logged In",
#    "firstName":     "Celeste",
#    "gender":        "F",
#    "itemInSession": 1,
#    "lastName":      "Williams",
#    "length":        277.15873,
#    "level":         "free",
#    "location":      "Klamath Falls, OR",
#    "method":        "PUT",
#    "page":          "NextSong",
#    "registration":  1541077528796.0,
#    "sessionId":     438,
#    "song":          "Horn Concerto No. 4 in E flat K495: II. Romance (Andante cantabile)",
#    "status":        200,
#    "ts":            1541990264796,
#    "userAgent":     "\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.103 Safari\/537.36\"",
#    "userId":        "53"
#}    

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id        INT IDENTITY(0,1),
    artist          VARCHAR(255),
    auth            VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender     VARCHAR(1),
    item_in_session INTEGER,
    last_name       VARCHAR(255),
    song_duration   FLOAT, 
    user_level      VARCHAR(50),
    user_location   VARCHAR(255),
    method          VARCHAR(25),
    page            VARCHAR(35),
    registration    VARCHAR(50),
    session_id      BIGINT,
    song            VARCHAR(255),
    status          INTEGER, 
    ts              TIMESTAMP,
    user_agent      TEXT,
    user_id         INTEGER,
    PRIMARY KEY (event_id))
""")

# TRAAAAW128F429D538.json file looks like : 
#    "num_songs":        1, 
#    "artist_id":        "ARD7TVE1187B99BFB1", 
#    "artist_latitude":  null, 
#    "artist_longitude": null, 
#    "artist_location":  "California - LA", 
#    "artist_name":      "Casual", 
#    "song_id":          "SOMZWCG12A8C13C480", 
#    "title":            "I Didn't Mean To", 
#    "duration":         218.93179, 
#    "year":             0

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id              VARCHAR(100),
    num_songs            INTEGER,
    song                 VARCHAR(255),
    artist_id            VARCHAR(100),
    artist_latitude      FLOAT,
    artist_longitude     FLOAT,
    artist_location      VARCHAR(255),
    artist               VARCHAR(255),
    song_duration        FLOAT,
    year                 INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id   INT IDENTITY(0,1),
    start_time    TIMESTAMP,
    user_id       INTEGER            NOT NULL,
    song_id       VARCHAR(100)       NOT NULL,
    artist_id     VARCHAR(100),
    session_id    BIGINT,
    user_level    VARCHAR(50),
    user_location VARCHAR(255),
    user_agent    TEXT,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id     INTEGER,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    user_gender VARCHAR(1),
    user_level  VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id       VARCHAR(100),
    song          VARCHAR(255),
    artist_id     VARCHAR(100) NOT NULL,
    year          INTEGER,
    song_duration FLOAT,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id        VARCHAR(100),
    artist           VARCHAR(255),
    artist_location  VARCHAR(255),
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, user_level, user_location, user_agent) 
SELECT DISTINCT
    TO_TIMESTAMP(SE.ts, 'YYYY-MM-DD HH24:MI:SS') AS start_time,
    SE.user_id, 
    SS.song_id,
    SS.artist_id, 
    SE.session_id,
    SE.user_level, 
    SE.user_location, 
    SE.user_agent
FROM 
  staging_events AS SE
INNER JOIN
  staging_songs AS SS
  USING (song, artist)
WHERE SE.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, user_gender, user_level)
SELECT DISTINCT  
    user_id, 
    first_name, 
    last_name, 
    user_gender, 
    user_level
FROM staging_events
WHERE
    user_id IS NOT NULL
    AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, song, artist_id, year, song_duration) 
SELECT DISTINCT 
    song_id, 
    song,
    artist_id,
    year,
    song_duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
    artist_id,
    artist,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
FROM songplays
WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
