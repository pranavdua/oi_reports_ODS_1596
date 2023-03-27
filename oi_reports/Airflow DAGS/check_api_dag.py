from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_api_dag',
    default_args=default_args,
    description='DAG that calls an API',
    schedule_interval='*/1 * * * *',
)

def call_api():
    response = requests.get('http://127.0.0.1:5000/next_run_times')
    data = response.json()
    format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(list(data.values())[0], format)

def check_date_diff(**kwargs):
    ti = kwargs['ti']
    api_date = ti.xcom_pull(task_ids='call_api')
    api_date = datetime.strptime(api_date, '%Y-%m-%d')
    today = datetime.today().date()
    time_diff = today - api_date.date()
    if time_diff < timedelta(days=1):
        return 'api_task'
    else:
        return 'done'

def run_another_api():
    response = requests.get('https://api.example.com/another_api')
    data = response.json()
    print(data)

call_api_task = PythonOperator(
    task_id='call_api',
    python_callable=call_api,
    dag=dag,
)

check_date_diff_task = PythonOperator(
    task_id='check_date_diff',
    provide_context=True,
    python_callable=check_date_diff,
    dag=dag,
)

run_another_api_task = PythonOperator(
    task_id='api_task',
    python_callable=run_another_api,
    dag=dag,
)

done_task = PythonOperator(
    task_id='done',
    python_callable=lambda: print('Done!'),
    dag=dag,
)

call_api_task >> check_date_diff_task >> [run_another_api_task, done_task]

dag.doc_md = """
### DAG that calls an API

This DAG calls an API and if the date returned by the API is less than 1 day ago, it runs another API.
"""

dag.doc_md += """
#### Triggering the DAG

The DAG can be manually triggered from the Airflow UI.
"""

dag.doc_md += """

To manually trigger the DAG, follow these steps:

1. Go to the Airflow UI
2. Navigate to the DAGs page
3. Click on the `my_api_dag` DAG
4. Click on the Trigger
