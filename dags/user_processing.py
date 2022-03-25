from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

import json
from datetime import datetime, timedelta
from pandas import json_normalize

def _storing():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )

def _processing_user(ti):
    users = ti.xcom_pull(task_ids='extracting_user')
    if not len(users) or 'results' not in users:
        raise ValueError('User is empty')
    user = users['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

DEFAULT_ARGS = {
    "retries": 3,
}

def _check_if_empty(ti):
    users = ti.xcom_pull(task_ids='extracting_user')
    if not len(users) or 'results' not in users:
        return 'is_empty'
    return 'is_ok'
    

with DAG("user_processing", default_args=DEFAULT_ARGS, schedule_interval="0 14 * * *", 
         start_date=datetime(2022, 3 , 24, 14, 0, 0), 
         catchup=False, description="Processing users", tags=['team-jean-paul', 'team-xavier'],
         template_searchpath=['/opt/airflow/include']) as dag:

    creating_table = PostgresOperator(
            task_id='creating_table',
            postgres_conn_id='postgres',
            sql='CREATE_TABLE_USERS.sql'
        )
    
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
    )
    
    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    
    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )
    
    storing = PythonOperator(
        task_id='storing',
        python_callable=_storing
    )
    
    check_if_empty = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=_check_if_empty
    )
    
    is_empty = DummyOperator(task_id="is_empty")
    is_ok = DummyOperator(task_id="is_ok")

    creating_table >> is_api_available >> extracting_user >> check_if_empty
    check_if_empty >> is_empty # error path
    check_if_empty >> is_ok >> processing_user >> storing # happy path