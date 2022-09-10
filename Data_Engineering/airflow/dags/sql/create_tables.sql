CREATE TABLE public.artists (
	artist_id varchar(256) NOT NULL,
	artist_name varchar(256),
	location varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE public.songplays (
	songplay_id varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	user_id int32 NOT NULL,
	"level" varchar(256),
	song_id varchar(256),
	artist_id varchar(256),
	session_id int32,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);

CREATE TABLE public.songs (
	song_id varchar(256) NOT NULL,
	song_title varchar(256),
	artist_id varchar(256),
	"year" int32,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);

CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int32,
	song varchar(256),
	status int32,
	ts int32,
	useragent varchar(256),
	userid int32
);

CREATE TABLE public.staging_songs (
	num_songs int32,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int32
);

CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int32,
	"day" int32,
	week int32,
	"month" varchar(256),
	"year" int32,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE public.users (
	user_id int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (user_id)
);
