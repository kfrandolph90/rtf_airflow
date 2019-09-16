from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import time
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor

from RTF.rtf_utils.rtf_utils import copy_blob, list_blobs, delete_blob
import logging

watch_bucket = "sixty-analytics"
dest_bucket = "rtf_staging"

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 8, 1), 
    'email_on_failure': False,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 0, 
    'retry_delay': timedelta(minutes=20),
    'provide_context':True # this makes xcom work
}

dag = DAG('Move_DataLab_Export',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval=None, 
            catchup=False)

def fetch_blobs_list(**context):
    #blobs = context['task_instance'].xcom_pull(task_ids='blob_exist_check')
    blobs = list(map(lambda blob: blob.name, list_blobs("sixty-analytics",prefix="rtf_")))   
    logging.info(blobs)
    return blobs

def copy_all_blobs(**context):
    blob_names = context['task_instance'].xcom_pull(task_ids='list_blobs_task')
    for blob_name in blob_names:
        logging.info('Copying blob : %s', blob_name)
        copy_blob(bucket_name=watch_bucket,blob_name=blob_name,
                    new_bucket_name=dest_bucket, new_blob_name=blob_name)


def cleanup_all_blobs(**context):
    blob_names = context['task_instance'].xcom_pull(task_ids='list_blobs_task')
    for blob_name in blob_names:
        logging.info('Deleting blob : %s', blob_name)
        delete_blob(bucket_name=watch_bucket,blob_name=blob_name)

start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id= "End", retries=0, dag=dag)

blob_exists_check=GoogleCloudStoragePrefixSensor(
    task_id='blob_exist_check',
    bucket=watch_bucket,prefix="rtf_",
    poke_interval=30, timeout=10, soft_fail=True, 
    provide_context=True,
    dag=dag)
            
blob_list_task=PythonOperator(task_id='list_blobs_task', python_callable=fetch_blobs_list, dag=dag,provide_context=True)
blob_move_task=PythonOperator(task_id='copy_blobs_task', python_callable=copy_all_blobs, dag=dag,provide_context=True)
blob_delete_task=PythonOperator(task_id='delete_blobs_task', python_callable=cleanup_all_blobs, dag=dag,provide_context=True)
                    
start_task >> blob_exists_check >> blob_list_task>> blob_move_task>> blob_delete_task >> end_task