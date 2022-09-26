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
            EXTRACT(hour from start_time),
            EXTRACT(day from start_time),
            EXTRACT(week from start_time),
            EXTRACT(month from start_time),
            EXTRACT(year from start_time),
            EXTRACT(dayofweek from start_time)
        FROM songplays
    """)
    
    dim_tables_map = {
        'users': user_table_insert,
        'songs': song_table_insert,
        'artists': artist_table_insert,
        'time': time_table_insert
    }

    create_tables = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
            artist_id   VARCHAR(256) NOT NULL,
            artist_name VARCHAR(256),
            location    VARCHAR(256),
            latitude    NUMERIC(18,0),
            longitude   NUMERIC(18,0)
        );

        CREATE TABLE IF NOT EXISTS public.songplays (
            songplay_id VARCHAR(32) NOT NULL,
            start_time  TIMESTAMP NOT NULL,
            user_id     INT4 NOT NULL,
            "level"     VARCHAR(256),
            song_id     VARCHAR(256),
            artist_id   VARCHAR(256),
            session_id  INT4,
            location    VARCHAR(256),
            user_agent  VARCHAR(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
        );

        CREATE TABLE IF NOT EXISTS public.songs (
            song_id     VARCHAR(256) NOT NULL,
            song_title  VARCHAR(256),
            artist_id   VARCHAR(256),
            "year"      INT4,
            duration    NUMERIC(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (song_id)
        );

        CREATE TABLE IF NOT EXISTS public.staging_events (
            artist          VARCHAR(256),
            auth            VARCHAR(256),
            firstname       VARCHAR(256),
            gender          VARCHAR(256),
            iteminsession   INT4,
            lastname        VARCHAR(256),
            length          NUMERIC(18,0),
            "level"         VARCHAR(256),
            location        VARCHAR(256),
            "method"        VARCHAR(256),
            page            VARCHAR(256),
            registration    NUMERIC(18,0),
            sessionid       INT8,
            song            VARCHAR(256),
            status          INT8,
            ts              INT8,
            useragent       VARCHAR(256),
            userid          INT4
        );

        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs           INT4,
            artist_id           VARCHAR(256),
            artist_name         VARCHAR(256),
            artist_latitude     NUMERIC(18,0),
            artist_longitude    NUMERIC(18,0),
            artist_location     VARCHAR(256),
            song_id             VARCHAR(256),
            title               VARCHAR(256),
            duration            NUMERIC(18,0),
            "year"              INT4
        );

        CREATE TABLE IF NOT EXISTS public."time" (
            start_time  TIMESTAMP NOT NULL,
            "hour"      INT4,
            "day"       INT4,
            week        INT4,
            "month"     VARCHAR(256),
            "year"      INT4,
            weekday     VARCHAR(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );

        CREATE TABLE IF NOT EXISTS public.users (
            user_id     INT4 NOT NULL,
            first_name  VARCHAR(256),
            last_name   VARCHAR(256),
            gender      VARCHAR(256),
            "level"     VARCHAR(256),
            CONSTRAINT users_pkey PRIMARY KEY (user_id)
        );
    """)