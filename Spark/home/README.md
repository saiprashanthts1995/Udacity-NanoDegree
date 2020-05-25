# Project: Data Modeling with Postgres
## Description

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. **users** - users in the app user_id, first_name, last_name, gender, level
2. **songs** - songs in music database song_id, title, artist_id, year, duration
3. **artists** - artists in music database artist_id, name, location, lattitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## Folder Contents
```bash
1. data Folder  --> Contains the data file
2. dl.cfg --> Contains the credentials details like 
3. etl.py --> Contains the Code
4. README.MD --> Contains the instruction
```

## AWS Setup
1. Create an .ppk file using EC2 console
2. Create the EMR cluster 
    1. Having 2 Nodes Mx4.large
    2. Configuration as spark + yarn
    3. In the above created cluster default role will created along with that add s3 access role as well
3. Open the Security port for the above EMR (EC2 instance) for SSH port 22
4. Use WInscp to connect to EMR console
5. start developing the code

## Development Process

Following Steps where performed to complete the project
1. Complete the AWS Setup
2. Start typing the code as per the guidelines by reading data and loading the data into s3
3. dataframe will be written as partition of files
    for example,s3://Bucket_name/Song_Table/year=1969/artist_id=ARMJAGH1187FB546F3/part-00006-ac9ff2ef-22b0-4a75-a2ce-dd49f7aa3318.c000.snappy.parquet
    
    Advantage of these type of query is that when the data is filtered it will be more faster

Main Purpose of this project is to load the data into s3 as seperate folder
The list of tables are as follows
  1. songplays
  2. users
  3. songs
  4. artists
  5. time
    
## Steps to run the ETL Pipeline

```bash
1. Run etl.py to load the data after performing ETL Operations
    Command to run spark-submit etl.py
2. Check the data using AWS S3 console
```

## Purpose 
This project is done as part of Udacity Nano Degree project

## Done By
Sai Prashanth Thalanayar Swaminathan
    saiprashanthts@gmail.com