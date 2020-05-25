import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE if exists STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE if exists STAGING_SONGS"
songplay_table_drop = " DROP TABLE if exists SONGPLAYS"
user_table_drop = "DROP TABLE if exists USERS"
song_table_drop = "DROP TABLE if exists SONGS"
artist_table_drop = "DROP TABLE if exists ARTISTS"
time_table_drop = "DROP TABLE if exists TIME"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE STAGING_EVENTS(
    EVENT_ID INT IDENTITY(0,1) PRIMARY KEY,
    ARTIST_NAME VARCHAR,
    AUTH VARCHAR,
    USER_FIRST_NAME VARCHAR,
    USER_GENDER  VARCHAR,
    ITEM_IN_SESSION INT,
    USER_LAST_NAME VARCHAR,
    SONG_LENGTH FLOAT, 
    USER_LEVEL VARCHAR,
    LOCATION VARCHAR,  
    METHOD VARCHAR,
    PAGE VARCHAR,   
    REGISTRATION VARCHAR,   
    SESSION_ID  BIGINT,
    SONG_TITLE VARCHAR,
    STATUS INT,
    TS VARCHAR,
    USER_AGENT VARCHAR,    
    USER_ID VARCHAR);
""")

staging_songs_table_create = ("""CREATE TABLE  IF NOT EXISTS STAGING_SONGS(
    SONG_ID VARCHAR PRIMARY KEY NOT NULL,
    NUM_SONGS INT,
    ARTIST_ID VARCHAR,
    ARTIST_LATITUDE FLOAT,
    ARTIST_LONGITUDE FLOAT,
    ARTIST_LOCATION VARCHAR,
    ARTIST_NAME VARCHAR,
    TITLE VARCHAR,
    DURATION FLOAT,
    YEAR INT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS SONGPLAYS 
(
SONGPLAY_ID int identity(0,1) , 
START_TIME TIMESTAMP,
USER_ID VARCHAR NOT NULL, 
LEVEL VARCHAR, 
SONG_ID VARCHAR, 
ARTIST_ID VARCHAR, 
SESSION_ID INT, 
LOCATION VARCHAR, 
USER_AGENT VARCHAR,
PRIMARY KEY(SONGPLAY_ID)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS  USERS
(
USER_ID VARCHAR PRIMARY KEY  NOT NULL, 
FIRST_NAME VARCHAR NOT NULL, 
LAST_NAME VARCHAR NOT NULL, 
GENDER VARCHAR, 
LEVEL VARCHAR
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS SONGS
(
SONG_ID VARCHAR PRIMARY KEY   NOT NULL,
TITLE VARCHAR,
ARTIST_ID VARCHAR,
YEAR INT,
DURATION FLOAT
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS ARTISTS
(
ARTIST_ID VARCHAR PRIMARY KEY  NOT NULL,
NAME VARCHAR,
LOCATION VARCHAR,
LATITUDE FLOAT,
LONGITUDE FLOAT
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME
(
START_TIME TIMESTAMP NOT NULL PRIMARY KEY ,
HOUR INT,
DAY INT,
WEEK INT, 
MONTH INT, 
YEAR INT, 
WEEKDAY INT
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY STAGING_EVENTS FROM '{file_path}'
 IAM_ROLE '{ARN}'
 REGION 'US-WEST-2' 
 COMPUPDATE OFF STATUPDATE OFF
 JSON '{json_path}'
""").format(file_path = config.get('S3','LOG_DATA'),
                        ARN = config.get('IAM_ROLE', 'ARN'),
                        json_path = config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY STAGING_SONGS FROM '{file_path}'
    IAM_ROLE '{ARN}'
    REGION 'US-WEST-2' 
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
""").format(file_path = config.get('S3','SONG_DATA'), 
                ARN = config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO SONGPLAYS (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT) 
    SELECT DISTINCT 
        TIMESTAMP 'EPOCH' + TS/1000 *INTERVAL '1 SECOND' AS START_TIME, 
        SE.USER_ID, 
        SE.USER_LEVEL,
        SS.SONG_ID,
        SS.ARTIST_ID,
        SE.SESSION_ID,
        SE.LOCATION,
        SE.USER_AGENT
    FROM STAGING_EVENTS SE 
        JOIN STAGING_SONGS SS
    ON SE.ARTIST_NAME  = SS.ARTIST_NAME
    WHERE UPPER(SE.PAGE) = 'NEXTSONG';
""")

user_table_insert = ("""INSERT INTO USERS (USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL)  
    SELECT DISTINCT 
        SS.USER_ID ,
        SS.USER_FIRST_NAME,
        SS.USER_LAST_NAME,
        SS.USER_GENDER, 
        SS.USER_LEVEL
    FROM STAGING_EVENTS SS
    WHERE UPPER(SS.PAGE) = 'NEXTSONG';
""")

song_table_insert = ("""INSERT INTO SONGS (SONG_ID, TITLE, ARTIST_ID, YEAR, DURATION) 
    SELECT DISTINCT 
        SS.SONG_ID, 
        SS.TITLE,
        SS.ARTIST_ID,
        SS.YEAR,
        SS.DURATION
    FROM STAGING_SONGS SS;
""")

artist_table_insert = ("""INSERT INTO ARTISTS (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE) 
    SELECT DISTINCT 
        SS.ARTIST_ID,
        SS.ARTIST_NAME,
        SS.ARTIST_LOCATION,
        SS.ARTIST_LATITUDE,
        SS.ARTIST_LONGITUDE
    FROM STAGING_SONGS SS;
""")

time_table_insert = ("""INSERT INTO TIME (START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY)
    SELECT 
        DERIVED_TABLE.START_TIME, 
        EXTRACT(HR FROM DERIVED_TABLE.START_TIME) AS HOUR,
        EXTRACT(D FROM DERIVED_TABLE.START_TIME) AS DAY,
        EXTRACT(W FROM DERIVED_TABLE.START_TIME) AS WEEK,
        EXTRACT(MON FROM DERIVED_TABLE.START_TIME) AS MONTH,
        EXTRACT(YR FROM DERIVED_TABLE.START_TIME) AS YEAR, 
        EXTRACT(WEEKDAY FROM DERIVED_TABLE.START_TIME) AS WEEKDAY 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'EPOCH' + TS/1000 *INTERVAL '1 SECOND' AS START_TIME 
        FROM STAGING_EVENTS SE WHERE UPPER(SE.PAGE) = 'NEXTSONG'
    )DERIVED_TABLE;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

