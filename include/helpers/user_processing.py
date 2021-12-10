from pandas import json_normalize
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

def _check_if_empty(ti):
    users = ti.xcom_pull(task_ids='extracting_user')
    if not users:
        return 'is_empty'
    return 'processing_user'

def _storing():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename=Variable.get('user_csv_file')
    )

def _processing_user(ti):
    users = ti.xcom_pull(task_ids='extracting_user')
    if not len(users) or 'results' not in users:
        raise Value('Users are empty')
    user = users['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv(Variable.get('user_csv_file'), index=None, 
                          header=False)
    