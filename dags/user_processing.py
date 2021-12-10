from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

import json
from datetime import datetime, timedelta
from include.helpers.user_processing import _processing_user, _storing, _check_if_empty

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'email': ['marc@sfr.fr'],
    'email_on_failure': True,
    'email_on_retry': True
}

with DAG('user_processing',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    description='Processing user',
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    template_searchpath=['/opt/airflow/include']
) as dag:

    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='postgres',
        sql='sql/CREATE_TABLE_USERS.sql',
        execution_timeout=timedelta
    )
    
    is_available_api = HttpSensor(
        task_id='is_available_api',
        http_conn_id='user_api',
        endpoint='api/',
        poke_interval=60,
        timeout=10,
        soft_fail=False,
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    check_if_empty = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=_check_if_empty
    )
    
    is_empty = PythonOperator(
        task_id='is_empty'
    )

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    storing = PythonOperator(
        task_id='storing',
        python_callable=_storing
    )
    
    #label_extract = Label("user extracted")
    creating_table >> is_available_api >> extracting_user
    extracting_user >> check_if_empty >> [is_empty, processing_user]
    processing_user >> storing