from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG('scheduling', schedule_interval='@daily',
         start_date=datetime(2020, 1, 1),
         catchup=False) as dag:
    
    t1 = BashOperator(
        task_id='t1',
        bash_command='exit 1'
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command='sleep 10'
    )
    
    t3 = BashOperator(
        task_id='t3',
        bash_command='sleep 10'
    )
    
    t4 = BashOperator(
        task_id='t4',
        bash_command='sleep 10'
    )
    
    [t1, t2, t3] >> t4
    