import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator


# Config variables
BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "mahendrainsite"
BQ_DATASET = "github_trends"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2020, 7, 1),
    'end_date': datetime(2020, 7, 5),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 21 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'bigquery_github_trends', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

## Task 1: check that the github archive data has a dated table created for that date
t1 = BigQueryCheckOperator(
        task_id='bq_check_githubarchive_day',
        sql='''
        #standardSQL
        SELECT
          table_id
        FROM
          `githubarchive.day.__TABLES_SUMMARY__`
        WHERE
          table_id = "{{ yesterday_ds_nodash }}"
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )

## Task 2: check that the hacker news table contains data for that date.
t2 = BigQueryCheckOperator(
        task_id='bq_check_hackernews_full',
        sql='''
        #standardSQL
        SELECT
          FORMAT_TIMESTAMP("%Y%m%d", timestamp ) AS date
        FROM
          `bigquery-public-data.hacker_news.full`
        WHERE
          type = 'story'
          AND FORMAT_TIMESTAMP("%Y%m%d", timestamp ) = "{{ yesterday_ds_nodash }}"
        LIMIT
          1
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )


# Setting up Dependencies
t2.set_upstream(t1)

