from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

from datetime import datetime
from pandas import json_normalize

def _processing_user():
    users = None
    if not len(users) or 'results' not in users:
        raise Value('Users are empty')
    user = users['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['lastname'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, 
                          header=False)
    

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

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )
