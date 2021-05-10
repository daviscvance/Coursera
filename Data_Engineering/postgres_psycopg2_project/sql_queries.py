# DROP TABLES ##################################################################

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES ################################################################

# Records in log data associated with song plays (records with page NextSong).
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL     PRIMARY KEY,
        start_time  TIMESTAMP  NOT NULL,
        user_id     INT,
        level       VARCHAR,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  INT,
        location    VARCHAR,
        user_agent  VARCHAR
    );
""")

# Users in the app.
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INT      PRIMARY KEY,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        level      VARCHAR
    );
""")

# Songs in music database. 
# Wanted to add "REFERENCES artists (artist_id)" here but it does not exist in 
# artist table and throws error.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   VARCHAR  PRIMARY KEY,
        title     VARCHAR,
        artist_id VARCHAR  NOT NULL,
        year      INT,
        duration  FLOAT
    );
""")

# Artists in music database.
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name      VARCHAR,
        location  VARCHAR,
        latitude  FLOAT,
        longitude FLOAT
    );
""")

# Timestamps of records in songplays broken down into specific units.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    INT
    );
""")

# INSERT RECORDS ###############################################################

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id)
        DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
        DO UPDATE
        SET 
        (first_name, last_name, gender, level) = (
            EXCLUDED.first_name, 
            EXCLUDED.last_name, 
            EXCLUDED.gender, 
            EXCLUDED.level);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
        DO UPDATE
        SET 
        (title, artist_id, year, duration) = (
            EXCLUDED.title,
            EXCLUDED.artist_id,
            EXCLUDED.year,
            EXCLUDED.duration);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
        DO UPDATE
        SET 
        (name, location, latitude, longitude) = (
            EXCLUDED.name,
            EXCLUDED.location,
            EXCLUDED.latitude,
            EXCLUDED.longitude);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
        DO UPDATE
        SET 
        (hour, day, week, month, year, weekday) = (
            EXCLUDED.hour, 
            EXCLUDED.day, 
            EXCLUDED.week, 
            EXCLUDED.month,
            EXCLUDED.year,
            EXCLUDED.weekday);
""")

# FIND SONGS ###################################################################

# Get songid and artistid from song and artist tables.

song_select = ("""
    SELECT 
        S.song_id,
        A.artist_id
    FROM songs AS S
    LEFT JOIN
        artists AS A
        ON S.artist_id = A.artist_id
    WHERE
        S.title = %s
        AND A.name = %s
""")

# QUERY LISTS ##################################################################

create_table_queries = [songplay_table_create, artist_table_create, 
                        song_table_create, user_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]

