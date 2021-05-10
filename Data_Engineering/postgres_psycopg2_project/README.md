# SparkifyDB

## Overview

This pipeline processes JSON song files and log data to provide a holistic view of user's music streaming activity in Sparkify. Here we create a Postgres database with a start schema centered around a `songplays` fact table. We include dimensional tables: users, songs, artists, time for metadata to query that will enhance our understanding of user behaviors.

## Schema

### Songplays

Records in log data associated with song plays (records with page NextSong).

| column      | type      | Primary keys |
|-------------|-----------|--------------|
| songplay_id | Serial    | *            |
| start_time  | TIMESTAMP |              |
| user_id     | INT       |              |
| level       | VARCHAR   |              |
| song_id     | VARCHAR   |              |
| artist_id   | VARCHAR   |              |
| session_id  | INT       |              |
| location    | VARCHAR   |              |
| user_agent  | VARCHAR   |              |

### Users

Users in the app.

| column     | type    | Primary keys |
|------------|---------|--------------|
| user_id    | INT     | *            |
| first_name | VARCHAR |              |
| last_name  | VARCHAR |              |
| gender     | VARCHAR |              |
| level      | VARCHAR |              |

#### Songs

Songs in the music database.

| column     | type    | Primary keys |
|------------|---------|--------------|
| song_id    | VARCHAR | *            |
| title      | VARCHAR |              |
| artist_id  | VARCHAR |              |
| year       | VARCHAR |              |
| duration   | FLOAT   |              |         

# Artists

Artists in music database.

| column     | type    | Primary keys |
|------------|---------|--------------|
| artist_id  | VARCHAR | *            |
| name       | VARCHAR |              |
| location   | VARCHAR |              |
| latitud    | FLOAT   |              |
| longitude  | FLOAT   |              |

# Time

Timestamps of records in songplays broken down into specific units.

| column     | type      | Primary keys |
|------------|-----------|--------------|
| start_time | TIMESTAMP | *            |
| hour       | INT       |              |
| day        | INT       |              |
| week       | INT       |              |
| month      | INT       |              |
| year       | INT       |              |
| weekday    | INT       |              |

## ETL

Song metadata comes from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Example file: `song_data/A/B/C/TRABCEI128F424C983.json`
Example contents:
```JSON
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```
The user logs are also in JSON format and contain data on user actions and relevant metadata.
Example filepath:
Example contents: (a snippet of the NextSong action which we process for.)
```JSON
...
{
    "artist":"Survivor",
    "auth":"Logged In",
    "firstName":"Jayden",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Fox",
    "length":245.36771,
    "level":"free",
    "location":"New Orleans-Metairie, LA",
    "method":"PUT",
    "page":"NextSong",
    "registration":1541033612796.0,
    "sessionId":100,
    "song":"Eye Of The Tiger",
    "status":200,
    "ts":1541110994796,
    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"101"
}
...
```

## Query Cookbook

Query to get the songs listened to by each user:

```SQL
SELECT
  U.first_name,
  U.last_name,
  S.title AS song,
  A.name AS artist,
  COUNT(DISTINCT songplay_id) AS listens,
  MIN(start_time) AS first_listen,
  MAX(start_time) AS last_listen
FROM songplays AS SP
LEFT JOIN
  users AS U
  ON SP.user_id = U.user_id
LEFT JOIN
  songs AS S
  ON SP.song_id = S.song_id
LEFT JOIN
  artists AS A
  ON SP.artist_id = A.artist_id
GROUP BY 
  U.first_name,
  U.last_name,
  S.title,
  A.name
```
