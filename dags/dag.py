import os
from datetime import timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from download import save,update

default_args = {
    'owner': 'Filip Chrzuszcz',
    'depends_on_past': False,
    'email': ['filipchrzuszcz1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'download',
    default_args=default_args,
    description='First data download or update data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['download or update']) as dag:
    t1 = PythonOperator(
        task_id='download_data',
        python_callable=save,
    )
    t2 = PythonOperator(
        task_id='update_data',
        python_callable=update,
    )
t1 >> t2
