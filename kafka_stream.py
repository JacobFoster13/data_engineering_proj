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
def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

# need to format the data in a specific manner
# task needs to be added to the Kafka queue so must have the  decorator on it
def format_data(res):
    user_data = {}

    # data extraction from JSON object
    user_data['first_name'] = res['name']['first']
    user_data['last_name'] = res['name']['last']
    user_data['gender'] = res['gender']
    user_data['zipcode'] = res['location']['postcode']
    user_data['email '] = res['email']
    user_data['username'] = res['login']['username']
    user_data['password'] = res['login']['sha256']
    user_data['salt'] = res['login']['salt']
    user_data['dob'] = res['dob']['date']
    user_data['registration_date'] = res['registered']['date']
    user_data['phone'] = res['phone']
    user_data['photo'] = res['picture']['medium']

    return user_data


def stream_data():
    res = get_data()
    res = format_data(res)    
    print(res)


# create a DAG (Directed Acyclic Graph) that triggers tasks in sequential order
# 
with DAG('user_automation', default_args=default_args, schedule='@daily', catchup=False) as dag:
    streamer = PythonOperator(
        task_id='api_stream',
        python_callable=stream_data
    )


stream_data()