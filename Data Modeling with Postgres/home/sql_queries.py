# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = " drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id Serial PRIMARY KEY NOT NULL, 
start_time bigint, user_id int NOT NULL, 
LEVEL varchar, 
song_id varchar, 
artist_id varchar, 
session_id int, 
LOCATION varchar, 
user_agent varchar
);
""")

user_table_create = ("""
create table if not exists  users
(
user_id int Primary KEY NOT NULL, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender varchar, 
level varchar
);
""")

song_table_create = ("""
create table if not exists songs
(
song_id varchar primary key NOT NULL,
title varchar,
artist_id varchar,
year int,
duration float
);
""")

artist_table_create = ("""
create table if not exists artists
(
artist_id varchar primary key NOT NULL,
name varchar,
location varchar,
latitude float,
longitude float
);
""")

time_table_create = ("""
create table if not exists time
(
start_time bigint primary key NOT NULL,
hour int,
day int,
week int, 
month int, 
year int, 
weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert
    into
    songplays ( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent )
values (%s,%s,%s,%s,%s,%s,%s,%s) on
conflict do nothing;
""")

user_table_insert = ("""
insert
    into
    users ( user_id , first_name , last_name , gender , level )
values (%s,%s,%s,%s,%s) on
conflict(user_id) DO
UPDATE
SET
	level = excluded.level;
""")

song_table_insert = ("""
insert
    into
    songs ( song_id , title , artist_id , year , duration )
values (%s,%s,%s,%s,%s) on
conflict do nothing;
""")

artist_table_insert = ("""
insert
    into
    artists ( artist_id , name , location , latitude , longitude )
values (%s,%s,%s,%s,%s) on
conflict do nothing;
""")


time_table_insert = ("""
insert
    into
    time ( start_time , hour , day , week , month , year , weekday )
values (%s,%s,%s,%s,%s,%s,%s) on
conflict do nothing;
""")

# FIND SONGS

song_select = ("""
select
    songs.song_id,
    songs.artist_id
from
    songs songs
join artists artists on
    songs.artist_id = artists.artist_id
where
    songs.title = %s
    and artists.name = %s
    and ceil(songs.duration) = ceil(%s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]