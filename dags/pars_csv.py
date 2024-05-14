import os
import datetime
import time
import requests

import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.http_hook import HttpHook, PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago


def create_db_if_notexist():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS my_table (
        Year INT
        Industry_aggregation CHAR
        Industry_code INT
        Industry_name CHAR
        Units CHAR
        Variable_code CHAR
        Variable_name CHAR
        Variable_category CHAR
        Value CHAR
        Industry_code TEXT
    );
    """
    cursor.execute(create_table_query)


csv_file_path = 'test.csv'
PG_CONN_ID = 'your_postgres_conn'
PG_TABLE = 'my_table'

args = {
    'owner': 'Nick',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_csv_to_postgres(**context):

    df = pd.read_csv(csv_file_path)

    create_table_query = create_db_if_notexist()
    cursor.execute(create_table_query)

    print(df.head)


with DAG(
        'csv_to_postgres',
        default_args=args,
        schedule_interval=timedelta(days=1),
) as dag:
    """# Get Postgres connection
    get_postgres_conn = PythonOperator(
        task_id='get_postgres_conn',
        python_callable=get_postgres_conn_info,
        provide_context=True,
        dag=dag,
    )"""

    # Load CSV to Postgres
    load_csv = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_postgres,
        provide_context=True,
        dag=dag,
    )

    # Set task dependencies
    load_csv


"""def get_postgres_conn_info(**context):

    conn = context['params']['conn']
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.database
    )
"""
