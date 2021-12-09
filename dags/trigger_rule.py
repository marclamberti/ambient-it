from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from datetime import datetime

def _t2():
    raise AirflowSkipException
    
with DAG('trigger_rules', schedule_interval='59 23 L * *', start_date=datetime(2021, 1, 1), catchup=False) as dag:
    
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t2
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command='exit 0',
        trigger_rule='all_success'
    )
    
    t3 = BashOperator(
        task_id='t3',
        bash_command='sleep 2',
        trigger_rule='all_success'
    )
    
    #t4 = BashOperator(
    #    task_id='t4',
    #    bash_command='sleep 2',
    #    trigger_rule='all_success'
    #)

    [t1, t2] >> t3
    #[t1, t2] >> t4