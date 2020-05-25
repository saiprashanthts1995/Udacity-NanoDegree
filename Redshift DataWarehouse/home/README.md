# Project: Data Warehouse
## Description

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Folder Contents
```bash
1. dwh.cfg
2. sqlqueries.py
3. etl.py
4. create_tables.py
```

## Development Process

Following Steps where performed to complete the project

Main Purpose of this project is to create and load the data into Redshift cluster
The list of tables are as follows
    1. songplays
    2. users
    3. songs
    4. artists
    5. time
    
When Creating the tables proper constraints like primary key and not null constraints has been defined

During the loading of data it is done using State table concept. First the stage table named 'STAGING_SONGS', 'STAGING_EVENTS' is created and using Redshift copy command the stage tables are loaded using the files present in s3 bucket. 

For an example,

COPY STAGING_SONGS FROM 's3://udacity-dend/song_data'
    IAM_ROLE ''
    REGION 'US-WEST-2' 
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'

In the above example STAGING_SONGS is loaded using the file present in S3.

COMPUPDATE IS SET AS OFF --> This is done to turn off the automatic compression

STATUPDATE IS SET AS OFF --> This is done to turn off the statistics update

Once the Data is loaded to Staging tables. We have to create the dimension and fact tables.

Dimension and Fact table is loaded using the above created stage table by making use of joins


### AWS Steps
1. Create an AWS Security Group 
    1. Navigate to EC2 Instance
    2. Allow Inbound rule as tcp protocal for Port 5439( Port where Redshift runs)
    3. Mention the ip range to allow as 0.0.0.0/0 (This is for learning purpose only. But in the production purpose make use of VPN)
2. Create an AWS IAM Role 
    1. Navigate to IAM
    2. create an role which allows full access to Redshift and Read Access to S3 (These two are present in the existing Roles )
3. Create an AWS Redshift cluster.While creating make sure you add above created IAM Role and Security Group
    1. for this project we have made use of 1 node dc2.large node 

### Steps to write the code

1. Made use of psycopyg2 package to connect to Redshift cluster from python
2. First an script called sql_queries.py is created which comprises of all drop and create statement
3. In the above script proper insert statement were statement were written which inserts data into dimension and fact table
4. Then created an script called create_tables.py which basically connects to redshift
5. In order to connect to Redshift, Configuration details are present in dwh.cfg file
6. Above script after connecting to Redshift executes the SQL queries sequentially 
7. Once all the steps are done and tables are created . Data is loaded using etl.py
7. In the script etl.py it performs all insert statement 


## Steps to run the ETL Pipeline

```bash
1. Run create_Tables.py to create database and tables
    ( DDL of table present in sql_queries.py)
    python3 create_tables.py
2. Run etl.py to load the data after performing ETL Operations
    python3 etl.py
```

## STATS

1. select count(*) from STAGING_EVENTS --8056
2. select count(*) from STAGING_SONGS; --14896
3. select count(*) from USERS; --104
4. select count(*) from ARTISTS; --10025
5. select count(*) from TIME; --6813
6. select count(*) from SONGS; -- 14896
7. Select count(*) from SONGPLAYS; -- 9957

## Purpose 
This project is done as part of Udacity Nano Degree project

## Done By
Sai Prashanth Thalanayar Swaminathan
    saiprashanthts@gmail.com