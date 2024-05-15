import os

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
    csv_path = os.path.join(dag_folder, "data/test.csv")

    # Ебашим датафрейм
    df = pd.read_csv(csv_path)

    with open(csv_path, 'r') as f:
        header = f.readline().strip().split(',')
    # logger.debug(header)
    print(header)
    # пушим данные в xcom
    ti.xcom_push(key='load_csv_posgres', value=df)


def into_data(**kwargs):
    ti = kwargs['ti']

    if ti is None:
        raise ValueError('ti тут нихуя нету')

    res_df = ti.xcom_pull(key='load_csv_posgres', task_ids='extract_data')
    print(res_df)

    if res_df is None:
        raise ValueError('res_df нихуя нет')

    # print(res_df.head())

    #  Работа с бд
    pg_hook = PostgresHook(postgres_conn_id='posgres_localhost')

    if isinstance(pg_hook, PostgresHook):
        print('Итс окэй')
    else:
        print('Иди нахуй')

    # Создание таблицы в PostgreSQL, если она еще не существует
    table_name = 'my_table'
    create_table_query = (f"CREATE TABLE IF NOT EXISTS {table_name} " +
                          "(year INT, industry_aggregation VARCHAR(10), " +
                          "industry_code INT, industry_name VARCHAR(125), " +
                          "units VARCHAR(55), varibale_code VARCHAR(125), " +
                          "varibale_name VARCHAR(215), " +
                          "varibale_category VARCHAR(125), " +
                          "value DECIMAL, indastry_code VARCHAR(215));")

    conn = psycopg2.connect(dbname="airflow", user="airflow",
                            password="airflow", host="host.docker.internal")
    cursor = conn.cursor()
    cursor.execute(create_table_query)

    # Загрузка данных из дата фрейма в таблицу PostgreSQL
    # Эта хуйня в доработке
    insert_query = f"""INSERT INTO {table_name}
    (year, industry_aggregation, industry_code, industry_name,
    units, varibale_code, varibale_name, varibale_category,
    value, indastry_code)
    VALUES ({r'%s, '*10});"""
    execute_values(cursor, insert_query, res_df.to_numpy())

    # Фиксация изменений в базе данных
    conn.commit()

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()


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

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=into_data,
        provide_context=True,
        dag=dag
    )

    extract_csv >> load_data
