from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import os

DATE_TO_UPLOAD = "2022-08-11"
LOCAL_PATH = "/opt/airflow/files/raw/sales/" + DATE_TO_UPLOAD
GCS_BUCKET = "hm10_test_bucket"


GCS_PATH = f"src1/sales/v1/{datetime.strptime(DATE_TO_UPLOAD, '%Y-%m-%d').strftime('%Y/%m/%d')}"

def list_files(**kwargs):
    files = os.listdir(LOCAL_PATH)
    files = [os.path.join(LOCAL_PATH, file) for file in files if os.path.isfile(os.path.join(LOCAL_PATH, file))]
    kwargs['ti'].xcom_push(key='file_list', value=files)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='upload_files_to_gcs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

   
    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
        provide_context=True,
    )

    
    def create_upload_tasks(**kwargs):
        ti = kwargs['ti']
        files = ti.xcom_pull(task_ids='list_files', key='file_list')
        
        for file in files:
            file_name = os.path.basename(file)
            upload_task = LocalFilesystemToGCSOperator(
                task_id=f'upload_{file_name}',
                src=file,
                dst=f'{GCS_PATH}/{file_name}',
                bucket=GCS_BUCKET,
            )
            upload_task.execute(context=kwargs)

    create_upload_tasks_task = PythonOperator(
        task_id='create_upload_tasks',
        python_callable=create_upload_tasks,
        provide_context=True,
    )

    list_files_task >> create_upload_tasks_task
