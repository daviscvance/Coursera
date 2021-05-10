import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """Reads a JSON file into a DataFrame and inserts select data into the song and artist tables.
    
    Args:
        cur::object psycopg2 connection cursor.
        filepath::str JSON filepath for data/song_data directory.
        
    Returns:
        None
    """
    # Open song file.
    df = pd.read_json(filepath, lines=True)

    # Insert song record.
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = list(df[song_columns].values[0])
    cur.execute(song_table_insert, song_data)

    # Insert artist record.
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = list(df[artist_columns].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Reads a JSON file into a DataFrame and inserts select data into the time, user, and songplay tables.
    
    Args:
        cur::object psycopg2 connection cursor.
        filepath::str JSON filepath for data/log_data directory.
        
    Returns:
        None
    """
    # Open log file.
    df = pd.read_json(filepath, lines=True)

    # Filter by NextSong action.
    df = df[df.page == 'NextSong']

    # Convert timestamp column to datetime.
    df['start_time'] = pd.to_datetime(df['ts'], unit='ms')
    
    # Insert time data records.
    time_data = [
        df.start_time,
        df.start_time.dt.hour,
        df.start_time.dt.day,
        df.start_time.dt.week,
        df.start_time.dt.month,
        df.start_time.dt.year,
        df.start_time.dt.weekday
    ]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
#     time_insert = list(time_df.to_records(index=False))
    time_insert = [list(row) for row in time_df.itertuples(index=False)]
    cur.executemany(time_table_insert, time_insert)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
    
    # insert user records
    for i, row in user_df.iterrows():
        user_id, first_name, last_name, gender, level = row.userId, row.firstName, row.lastName, row.gender, row.level
        cur.execute(user_table_insert, row)

    # insert songplay records
    columns = ['start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'song', 'artist', 'length']
    for index, row in df[columns].iterrows():
        
        # get songid and artistid from song and artist tables
        print(f"Querying song: {row.song}, by artist: {row.artist}", end = "\r", flush=True)
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (row.start_time, row.userId, row.level, song_id, artist_id, 
                         row.sessionId, row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)        
        

def process_data(cur, conn, filepath, func):
    """Wrapper function to process the directories of JSON files.

    Args:
        cur::object psycopg2 connection cursor.
        conn::object psycopg2 connection.
        filepath::str JSON filepath for data/* directory.
        func::function Correct function that will process the given filepath.
        
    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        if 'checkpoint' in datafile:
            print(f'Not processing: {datafile}.')
            continue
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files), end = "\r", flush=True)


def main():
    """Executes script."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

