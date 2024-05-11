import json
import os
import time
import requests
from datetime import datetime, timedelta
from typing import Iterable

from airflow import DAG
from airflow.operators.python import PythonOperator


BASE_DIR = "/workspaces/hm2/Files/"

JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join(BASE_DIR, "raw", "sales")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales")

def run_job1(date, **kwargs):
    raw_data_path = kwargs.get('raw_data_path')
    raw_folder_name = date.strftime('%Y-%m-%d')
    raw_folder_path = os.path.join(raw_data_path, raw_folder_name)
    date_str=date.strftime('%Y-%m-%d')
    print("Starting job1:")
    resp = requests.post(
        url=f'http://172.22.0.8:{JOB1_PORT}/fetch_api_data',
        json={
            "date": date_str,
            "raw_dir": raw_folder_path
        }
    )
    assert resp.status_code == 201
    print("job1 completed!")
  

def run_job2(date, **kwargs):
    raw_data_path = kwargs.get('raw_data_path')
    stg_data_path = kwargs.get('stg_data_path')
    raw_folder_name = date.strftime('%Y-%m-%d')
    raw_folder_path = os.path.join(raw_data_path, raw_folder_name)
    stg_folder_name = date.strftime('%Y-%m-%d')
    stg_folder_path = os.path.join(stg_data_path, stg_folder_name)
    print("Starting job2:")
    resp = requests.post(
        url=f'http://172.22.0.8:{JOB2_PORT}/convert',
        
        json={
            "raw_dir": raw_folder_path,
            "stg_dir": stg_folder_path
        }
    )
    assert resp.status_code == 201
    print("job2 completed!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 11),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

start_date = default_args['start_date']
days_to_process = 3
processed_days = 0

with DAG(
        dag_id="process_sales",
        default_args=default_args,
        schedule = "0 1 * * *",
        tags=['HM_3'],
        catchup=True
) as dag:
    while processed_days < days_to_process:
        date_to_process = start_date - timedelta(days=processed_days)
        date_to_process_str = date_to_process.strftime('%Y-%m-%d')
        extract_data_from_api = PythonOperator(
            task_id=f'extract_data_for_date_{date_to_process_str}_iter_{processed_days}',
            python_callable=run_job1,
            op_kwargs={'date': date_to_process, 'raw_data_path': RAW_DIR},
            provide_context=True
    )
   
        convert_to_avro = PythonOperator(
            task_id=f'convert_data_for_date_{date_to_process_str}_iter_{processed_days}',
            python_callable=run_job2,
            op_kwargs={'date': date_to_process, 'raw_data_path': RAW_DIR, 'stg_data_path': STG_DIR},
            provide_context=True
    )
        processed_days += 1
        extract_data_from_api >> convert_to_avro
       


