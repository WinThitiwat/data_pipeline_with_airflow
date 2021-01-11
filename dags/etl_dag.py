from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from etl_subdag import run_data_quality_checks

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

START_DATE = datetime(2019, 1, 12)
DAG_ID = 'sparkify_etl_dag'

default_args = {
    'owner': 'sparkify_pipeline',
    'start_date': START_DATE,
    'depend_on_past': False,
    'retries': 3,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id=DAG_ID,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{year}/{month}/{full_date}-events.json",
    format_option="json_path",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="log_data/{year}/{month}/{full_date}-events.json",
    format_option="json_path",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

qa_check_task_id = 'Run_data_quality_checks'
run_quality_checks = SubDagOperator(
    subdag=run_data_quality_checks(
        parent_dag_name=DAG_ID,
        task_id=qa_check_task_id,
        redshift_conn_id='redshift',
        target_tables=['artists','songplays','songs','time','users'],
        start_date=START_DATE
    ),
    
    task_id=qa_check_task_id,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# group all staging table operator into one var
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

# set relations between tasks
start_operator >> stage_to_redshift

stage_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_tables

load_dimension_tables >> run_quality_checks

run_quality_checks >> end_operator
