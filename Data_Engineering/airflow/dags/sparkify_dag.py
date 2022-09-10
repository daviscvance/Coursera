import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (PostgresOperator)
from datetime import datetime, timedelta
from helpers import SqlQueries
from operators import (StageToRedshiftOperator, 
                       LoadFactOperator,
                       LoadDimensionOperator,
                       DataQualityOperator)

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 9, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *')

start_operator = PostgresOperator(
    task_id = 'start_execution',  
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = 'sql/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events', 
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    target_table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
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
    s3_key = 'song_data',
    format_option = 'auto',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_table = 'songplays',
    sql = SqlQueries.songplay_table_insert,
    refresh_table = False
)

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
    target_tables = [ 'songplays', 'songs', 'artists', 'time', 'users' ]
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