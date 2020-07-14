import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop =  "drop table if exists staging_events"
staging_songs_table_drop =   "drop table if exists staging_songs"
songplay_table_drop =        "drop table if exists songplays"
user_table_drop =            "drop table if exists users ;"
song_table_drop =            "drop table if exists songs ;"
artist_table_drop =          "drop table if exists artists ;"
time_table_drop =            "drop table if exists time ;"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        first_name           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        last_name            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        session_id           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        user_agent           VARCHAR,
        user_id              INTEGER 
    )
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
create table if not exists songplays(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER ,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR ,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")

user_table_create = ("""
create table if not exists users(
        user_id             INTEGER PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR
    )
""")

song_table_create = ("""
create table songs(
        song_id             VARCHAR PRIMARY KEY,
        title               VARCHAR ,
        artist_id           VARCHAR ,
        year                INTEGER ,
        duration            FLOAT
    )
""")

artist_table_create = ("""
create table if not exists artists(
        artist_id           VARCHAR  PRIMARY KEY,
        name                VARCHAR ,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

time_table_create = ("""
create table if not exists time(
        start_time          TIMESTAMP       NOT NULL PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
Insert Into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.user_id ,
                se.level ,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.location ,
                se.user_agent
from staging_events as se
Inner join staging_songs as ss on se.song = ss.title and se.artist = ss.artist_name;
""")

user_table_insert = ("""
Insert Into users(user_id, first_name, last_name, gender, level)
select distinct user_id ,
                first_name ,
                last_name ,
                gender,
                level
from staging_events
where user_id is not null;
""")

song_table_insert = ("""
Insert Into songs(song_id, title, artist_id, year, duration)
select distinct song_id,
                title,
                artist_id,
                year as year,
                duration
from staging_songs
where song_id is not null;
""")

artist_table_insert = ("""
Insert Into artists(artist_id, name, location, latitude, longitude)
select distinct artist_id,
                artist_name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
from staging_songs
where artist_id is not null;
""")

time_table_insert = ("""
Insert Into time(start_time, hour, day, week, month, year, weekday)
select distinct ts,
                extract(hour from ts),
                extract(day from ts),
                extract(week from ts),
                extract(month from ts),
                extract(year from ts),
                extract(weekday from ts)
from staging_events
where ts is not null;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
