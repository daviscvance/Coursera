import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
from helpers import SqlQueries
from operators import (StageToRedshiftOperator, 
                       LoadFactOperator,
                       LoadDimensionOperator,
                       DataQualityOperator)

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 9, 24),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'sparkify_dag',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '0 * * * *',
    max_active_runs = 1
)


start_operator = PostgresOperator(
    task_id = 'start_execution',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.create_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events', 
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    target_table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    region = 'us-west-2',
    format_option = 's3://udacity-dend/log_json_path.json',
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    target_table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/A/A/A',
    region = 'us-west-2',
    format_option = 'auto',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_table = 'songplays',
    sql = SqlQueries.songplay_table_insert,
)

# Dimensions tables could be loaded via a subdag (dim_subdag.py).
load_user_dimension_table = LoadDimensionOperator(
    task_id = 'load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_table = 'users',
    sql = SqlQueries.user_table_insert,
    refresh_table = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_table = 'songs',
    sql = SqlQueries.song_table_insert,
    refresh_table = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'load_artist_dim_table',
    dag = dag,   
    redshift_conn_id = 'redshift',
    target_table = 'artists',
    sql = SqlQueries.artist_table_insert,
    refresh_table = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_table = 'time',
    sql = SqlQueries.time_table_insert,
    refresh_table = False
)

run_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    data_quality_checks = [ 
        {
            'target_table': 'songplays', 
            'expected_count': 900000,
            'condition': True,
            'comparison': '>=',
            'test_rationale': 'Check minimum records in songplays.'
        },
        {
            'target_table': 'songs', 
            'expected_count': 300,
            'condition': True,
            'comparison': '>=',
            'test_rationale': 'Check minimum records in songs table.'
        },
        {
            'target_table': 'artists', 
            'expected_count': 300,
            'condition': True,
            'comparison': '>=',
            'test_rationale': 'Check minimum records in artists table.'
        },
        {
            'target_table': 'time', 
            'expected_count': 4600000,
            'condition': True,
            'comparison': '>=',
            'test_rationale': 'Check minimum records in time table.'
        },
        {
            'target_table': 'users', 
            'expected_count': 1500,
            'condition': True,
            'comparison': '>=',
            'test_rationale': 'Check minimum records in users table.'
        }
    ]
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

##########################################################
##  DAG Structure  #######################################
##########################################################

stage_to_redshift = [
    stage_events_to_redshift, 
    stage_songs_to_redshift
]

load_dimension_tables = [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
]

start_operator >> stage_to_redshift

stage_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_tables

load_dimension_tables >> run_quality_checks

run_quality_checks >> end_operator