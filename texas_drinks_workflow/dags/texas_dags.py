from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import requests
import pandas as pd
import os
url = "https://data.texas.gov/resource/naix-2893.json?taxpayer_zip=77036"

tx_dag = DAG(dag_id = 'get_tax_receipts', start_date=datetime.now() - timedelta(days = 1))

def retreive_receipts():
    response = requests.get(url)
    json = response.json()
    df = pd.DataFrame(json)
    df.to_csv('./records.csv')    
    return json

def find_receipts():
    print(os.getcwd())
    # df = pd.read_csv('./records.csv')
    # records = df.to_dict('records.csv')
    # return records

retreive_task = PythonOperator(task_id='retrieve_receipts', python_callable=retreive_receipts, dag=tx_dag)

find_receipts_task = PythonOperator(task_id='find_large_receipts', python_callable=find_receipts, dag=tx_dag)

retreive_task >> find_receipts_task