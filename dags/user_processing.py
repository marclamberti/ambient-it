from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor

from datetime import datetime

with DAG('user_processing',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    description='Processing user',
    catchup=False
) as dag:

    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
        '''
    )
    
    is_available_api = HttpSensor(
        task_id='is_available_api',
        http_conn_id='user_api',
        endpoint='api/',
        poke_interval=60,
        timeout=10,
        soft_fail=False,
    )

