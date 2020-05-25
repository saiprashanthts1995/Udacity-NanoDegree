# Project: Data Modeling with Postgres
## Description

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Folder Contents
```bash
1. data Folder
2. test.ipynb
3. sqlqueries.py
4. etl.py
5. etl.ipynb
6. create_tables.py
```

## Development Process

Following Steps where performed to complete the project

Main Purpose of this project is to create and load the data into Postgres tables
The list of tables are as follows
    1. songplays
    2. users
    3. songs
    4. artists
    5. time
    
When Creating the tables proper constraints like primary key and not null contraints has been defined

During the loading of data it is done using "On Conflict" concept. which means what happens if any conflicts occurs. 
Mostly for all tables if conflicts happens I have done nothing. But for the table "USER" if conflict occurs then Column "Level" is updated
    

### Extract 

Files were extracted recursively from the folders using os and Glob Package (Using Methods like os.walk(), os.abspath(), glob.glob())

### Transformation

Following steps are performed to cleanse the data set

```bash
Data Set:data/song_data

1. Song Data Frame
   1. Columns which are required for process are taken into consideration 
   2. No Duplicates is present in this dataframe
   3. Drop Duplicates method is applied to remove the duplicates if any
   4. NaN Values were converted to Null Values
2. Artist Data Frame
   1. Columns which are required for process are taken into consideration 
   2. No Duplicates is present in this dataframe
   3. Drop Duplicates method is applied to remove the duplicates if any
   4. NaN Values were converted to Null Values

Data Set: data/song_data

In this particular data I have filtered based on the following condition
             df = df[(df['page'] == "NextSong")]

3. Time Data Frame
   1. Column ts is converted to type datetime
   2. Using pandas datetime functions hour,month,day,week, etc. is extracted

4. User Data Frame
   1. Columns which are required for process are taken into consideration 
   2. Duplicates are present in this dataframe
   3. drop Duplicates method is applied to remove the duplicates if any

5. SongPlay Data Frame
   1.This is obtained by combining above data sets 

```

### Load

Above dataframes are loaded to Corresponding tables using psycopg2 package by creating Connection and Cursor object

## Steps to run the ETL Pipeline

```bash
1. Run create_Tables.py to create database and tables
    ( DDL of table present in sql_queries.py)
2. Runetl.py to load the data after performing ETL Operations
3. Make use of test.ipynb to check the content of the table
```

## Purpose 
This project is done as part of Udacity Nano Degree project

## Done By
Sai Prashanth Thalanayar Swaminathan
    saiprashanthts@gmail.com