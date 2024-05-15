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
import requests
# from core import start_logging

# logger = start_logging()

POSTGRES_CONN_ID = "airflow"

"""Задание: необходимо загрузить json в базу данных,
распарсить его до таблицы lamoda_orders,
после чего сделать выгрузку данных с объединением таблицы
lamoda_orders и view map_product_status

ответом считается ddl функция для загрузки
json в базу с последующим распарсиванием

и запрос который выдает финальный ответ
"""


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
}


def sender(**kwargs):

    TELEGRAM_TOKEN = '********'
    TEXT_MSG = 'Внимение! Помещение провертирвается!\nВремя провертривания 15 мин.\nВыйдете из помещения'
    chat_id_list = []  # список из chat_id пользователей телеграма
    for chat_id in chat_id_list:
        requests.get(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={chat_id}&text={TEXT_MSG}') # noqa


with DAG('json_join',
         default_args=default_args,
         catchup=False,
         schedule_interval=('0 9,12,15,17.30  *   *   * ')
         ) as dag:

    send_msg = PythonOperator(
        task_id='extract_data',
        python_callable=sender,
        provide_context=True,
        dag=dag
    )

    send_msg
