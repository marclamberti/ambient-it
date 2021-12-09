from airflow.models import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG('trigger_rules', schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False) as dag:
    
    t1 = BashOperator(
        task_id='t1',
        bash_command='sleep 2'
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command='sleep 2'
    )
    
    t3 = BashOperator(
        task_id='t3',
        bash_command='sleep 2'
    )
    

    