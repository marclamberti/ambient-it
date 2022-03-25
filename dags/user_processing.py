from airflow import DAG
from airflow.operators.python import PythonOperator
# Add an import here...

import json
from datetime import datetime
from pandas import json_normalize

def _processing_user(ti):
    # Add something here...
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

with DAG("user_processing", schedule_interval="0 14 * * *", 
         start_date=datetime(2022, 3 , 24, 14, 0, 0), 
         catchup=False, description="Processing users", tags=['team-jean-paul', 'team-xavier']) as dag:

    creating_table = PostgresOperator(
            task_id='creating_table',
            postgres_conn_id='postgres',
            sql='''
                CREATE TABLE users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
                );
                '''
        )