import os

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
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
    csv_path = os.path.join(dag_folder, "data/test.csv")

    # Ебашим датафрейм
    df = pd.read_csv(csv_path)

    with open(csv_path, 'r') as f:
        header = f.readline().strip().split(',')
    # logger.debug(header)
    print(header)
    # пушим данные в xcom
    ti.xcom_push(key='load_csv_posgres', value=df)


with DAG('csv_to_postgres',
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

    create_table = PostgresOperator(
        task_id='create_table_value_table',
        postgres_conn_id='database_PG',
        sql=("CREATE TABLE IF NOT EXISTS my_table " +
             "(year INT, industry_aggregation VARCHAR(10), " +
             "industry_code INT, industry_name VARCHAR(125), " +
             "units VARCHAR(55), varibale_code VARCHAR(125), " +
             "varibale_name VARCHAR(215), " +
             "varibale_category VARCHAR(125), " +
             "value DECIMAL, indastry_code VARCHAR(215));")
    )

    insert_into_table = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='database_PG',
        sql=[f"""INSERT INTO my_table VALUES(
            {{{{ti.xcom_pull(key='load_csv_posgres', task_ids=['extract_data'])[0].iloc[{i}]['Year']}}}},
            {{{{ti.xcom_pull(key='load_csv_posgres', task_ids=['extract_data'])[0].iloc[{i}]['Industry_aggregation']}}}},
            {{{{ti.xcom_pull(key='load_csv_posgres', task_ids=['extract_data'])[0].iloc[{i}]['Industry_code']}}}},
            {{{{ti.xcom_pull(key='load_csv_posgres', task_ids=['extract_data'])[0].iloc[{i}]['Industry_name']}}}},
            )""" for i in range(5)]

    )

    extract_csv >> create_table >> insert_into_table
