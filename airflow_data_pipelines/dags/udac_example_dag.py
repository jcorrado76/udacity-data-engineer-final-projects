from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    "end_date": datetime(2018, 11, 2)
}


def log_details(*args, **kwargs):
    ds = "{}".format(kwargs['ds'])
    next_ds = "{}".format(kwargs['next_ds'])

    logging.info(f"Execution date is {ds}")
    if next_ds:
        logging.info(f"Next run date on {next_ds}")


dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

list_task_operator = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{{ execution_date.year }}/{{ '{:02}'.format(execution_date.month) }}/{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}-{{ '{:02}'.format(execution_date.day) }}-events.json",
    json_path="s3://udacity-dend/log_json_path.json"
)

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )
#
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )
#
# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )
#
# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )
#
# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
