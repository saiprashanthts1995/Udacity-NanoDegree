from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 5, 24),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@hourly'
    #'schedule_interval': None
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

# A dummy operator to start the airflow dag
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# load the stage events redshift table using the user defined operator
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-01-events.json", 
    json="s3://udacity-dend/log_json_path.json"
)

# load the stage songs redshift table using the user defined operator
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/TRAAAAK128F9318786.json",
    json="auto"
)

# load the songplays redshift table using the user defined operator
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    table="public.songplays",
    sql_statement=SqlQueries.songplay_table_insert
)

# load the user redshift table using the user defined operator
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    table="public.users",
    sql_statement=SqlQueries.user_table_insert
)

# load the songs redshift table using the user defined operator
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    table="public.songs",
    sql_statement=SqlQueries.song_table_insert
)

# load the artists redshift table using the user defined operator
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    table="public.artists",
    sql_statement=SqlQueries.artist_table_insert
)

# load the time redshift table using the user defined operator
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    table="public.time",
    sql_statement=SqlQueries.time_table_insert
)

# CHeck the data quality of time table to check if the records loaded properly
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    table_name="public.time"
)

# End operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task Dependency
# this has been designed in an such way that all staging will run parallely
# All Dimension will run parallely
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
