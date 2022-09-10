class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays (
            songplay_id, start_time, user_id, level, song_id,
            artist_id, session_id, location, user_agent)
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid user_id,
            events.level,
            songs.song_id, 
            songs.artist_id, 
            events.sessionid session_id, 
            events.location, 
            events.useragent user_agent
        FROM 
          (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page = 'NextSong'
          ) events
        LEFT JOIN staging_songs songs
          ON
            events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration;
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            userid user_id,
            firstname first_name,
            lastname last_name,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, song_title, artist_id, year, duration)
        SELECT DISTINCT
            song_id,
            title song_title,
            artist_id,
            year,
            duration
        FROM staging_songs;
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs;
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
            start_time,
            EXTRACT(hour from start_time) hour,
            EXTRACT(day from start_time) day,
            EXTRACT(week from start_time) week,
            EXTRACT(month from start_time) month,
            EXTRACT(year from start_time) year,
            EXTRACT(dayofweek from start_time) weekday
        FROM songplays
    """)