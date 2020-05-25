import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    This Method is used to process song and artist file after performing cleaning process and also after formating the data types
    '''

    '''
    :param cur: Cursor Object to connect to postgres
    :param filepath: filepath containing the path of the source file
    '''

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record

    # Columns_list to filter the required columns.
    # After performing basic functionality data is inserted into table

    columns_list = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df.loc[:, columns_list]

    # To CHeck if any duplicates is present

    song_data_bool = song_data.duplicated().any()
    print(
        'No Duplicates Present in the file Song File' if song_data_bool == False else "Duplicates Present in FIle "
                                                                                      "Song. Removing it")

    song_data.drop_duplicates(inplace=True)
    song_data = song_data.where(song_data.notnull(), None)
    song_data = song_data.astype({"song_id": str, "title": str,
                                  "artist_id": str, "year": int,
                                  "duration": float})
    song_data = song_data.values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record

    # Columns_list to filter the required columns.
    # After performing basic functionality data is inserted into table

    columns_list = ['artist_id', 'artist_name', 'artist_location',
                    'artist_latitude', 'artist_longitude']
    artist_data = df.loc[:, columns_list]

    # To CHeck if any duplicates is present

    artist_data_bool = artist_data.duplicated().any()

    print(
        'No Duplicates Present in the file Artist File' if artist_data_bool == False else "Duplicates Present in FIle "
                                                                                          "Artist. Removing it")

    artist_data.drop_duplicates(inplace=True)
    artist_data = artist_data.where(df.notnull(), None)

    # Renaming the columns
    artist_data.columns = ['artist_id', 'name', 'location',
                           'latitude', 'longitude']
    artist_data = artist_data.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    This Method is used to process log file after performing cleaning process and also after formating the data types
    '''

    '''
    :param cur: Cursor Object to connect to postgres
    :param filepath: filepath containing the path of the source file
    '''

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df['page'] == "NextSong")]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records

    time_data = (df['ts'].values, t.dt.hour.values,
                 t.dt.day.values, t.dt.week.values, t.dt.month.values,
                 t.dt.year.values, t.dt.weekday.values)
    column_labels = ('timestamp', 'hour', 'day', 'week',
                     'month', 'year', 'weekday')

    # Converting the 2 list into Dict
    # Dict is used in DF

    input_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(input_dict)
    time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table

    # Columns_list to filter the required columns.
    # After performing basic functionality data is inserted into table

    column_list = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df.loc[:, column_list]

    # To CHeck if any duplicates is present

    bool_user_df = user_df.duplicated().any()

    print(
        'No Duplicates Present in the file User File' if bool_user_df == False else "Duplicates Present in FIle User. "
                                                                                    "Removing it")

    user_df.drop_duplicates(inplace=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables

        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record

        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    This Method is used to process data present in path recursively and calling subsequent functions to load data to the table
    '''

    '''
    :param cur: Cursor Object to connect to postgres
    :param conn: connection Object to connect to postgres
    :param filepath: filepath containing the path of the source file
    :param func: corresponding functions
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    This Method is used to connect to postgres by creating the connection object. 
    it calls the method process_Data to load the data to corresponding tables
    '''

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
