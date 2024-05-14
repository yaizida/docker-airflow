import os

from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd

# from core import start_logging

# logger = start_logging()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


def load_csv_to_postgres(**kwargs):
    """
    Loads data from a CSV file to a PostgreSQL table
    """

    # Получаем путь к директории DAG файла
    dag_folder = os.path.dirname(__file__)

    # Формируем полный путь к CSV файлу
    csv_path = os.path.join(dag_folder, "data/test.csv")

    # Ебашим датафрейм
    # df = pd.read_csv(csv_path)
    # logger.debug(df)

    with open(csv_path, 'r') as f:
        header = f.readline().strip().split(',')
    # logger.debug(header)
    print(header)
    # Создание SQL запроса для создания таблицы


with DAG('csv_to_postgres',
         default_args=default_args,
         schedule_interval=' *   *   *   *   * '
         ) as dag:

    # Загрузка CSV файла в PostgreSQL
    load_csv = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    load_csv
