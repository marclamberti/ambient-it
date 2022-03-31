from airflow import DAG
from datetime import datetime

with DAG(dag_id="user_processing", description="Processing users", default_args={}, 
         schedule_interval="0 14 * * *", start_date=datetime(2022, 1, 1), catchup=False, 
         tags=['team_a'],
         ) as dag:

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
    
    cleaning = DummyOperator(task_id="cleaning")

    creating_table >> is_api_available >> extracting_user >> check_if_empty
    check_if_empty >> [is_empty, cleaning] # error path
    check_if_empty >> is_ok >> processing_user >> storing # happy path