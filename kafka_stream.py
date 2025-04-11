import json
import requests
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# default arguments to be passed to Airflow DAG
default_args = {
    "owner": "airscholar",
    "start_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

# create function that will be called in the Airflow DAG to pipe data from the API 
def stream_data():
    res = requests.get('https://randomuser.me/api/')
    data = res.json()['results'][0]


# create a DAG (Directed Acyclic Graph) that triggers tasks in sequential order
# 
with DAG('user_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    streamer = PythonOperator(
        task_id='api_stream',
        python_callable=stream_data
    )