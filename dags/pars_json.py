import os
import pprint
import json

from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from psycopg2.extras import execute_values
import pandas as pd
import psycopg2
# from core import start_logging

# logger = start_logging()

POSTGRES_CONN_ID = "airflow"


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


def extract_data(**kwargs):

    ti = kwargs['ti']
    # Получаем путь к директории DAG файла
    dag_folder = os.path.dirname(__file__)

    # Формируем полный путь к CSV файлу
    json_path = os.path.join(dag_folder, "data/test.json")

    with open(json_path, 'r') as f:
        template = json.load(f)

    pprint.pprint(template)
    ti.xcom_push(key='join_databse', value=template)


def join_data(**kwargs):
    ti = kwargs['ti']
    res_json = ti.xcom_pull(key='join_databse', task_ids='extract_data')
    pprint.pprint(res_json)


with DAG('json_join',
         default_args=default_args,
         catchup=False,
         schedule_interval='*/1   *   *   *   * '
         ) as dag:

    # Извлекаем данные из csv файла
    extract_csv = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        dag=dag
    )

    join_in_databse = PythonOperator(
        task_id='join_data',
        python_callable=join_data,
        provide_callable=True,
        dag=dag
    )

    extract_csv >> join_in_databse
