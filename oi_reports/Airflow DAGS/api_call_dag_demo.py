import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'pranavdua',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 25, 2, 52, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Pranav',
    default_args=default_args,
    description='A DAG that calls an API and displays the returned date on the Airflow UI',
    schedule_interval=None,
    catchup=False
)

def call_api():
    response = requests.get('http://127.0.0.1:5000/next_run_times')
    format = '%Y-%m-%d %H:%M:%S'
    date_str = response.json()
    return datetime.strptime(list(date_str.values())[0], format)

get_date = PythonOperator(
    task_id='get_date',
    python_callable=call_api,
    dag=dag
)

display_date = BashOperator(
    task_id='display_date',
    bash_command='echo "{{ ti.xcom_pull(task_ids="get_date") }}"',
    dag=dag
)

get_date >> display_date
