from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from RTF.rtf_utils.rtf_utils import copy_blob, list_blobs

watch_bucket = "sixty-analytics"
dest_bucket = "rtf_staging"

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 8, 1), 
    'email_on_failure': False,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 2, 
    'retry_delay': timedelta(minutes=20),
    'provide_context':True # this makes xcom work with
}

dag = DAG('Move_DataLab_Export',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval='@once', 
            catchup=False)

def copy_blob_task(**op_kwargs):
    copy_blob(
        bucket_name=op_kwargs['bucket_name'], 
        blob_name=op_kwargs['blob_name'], 
        new_bucket_name=op_kwargs['new_bucket_name'], 
        new_blob_name=op_kwargs['new_blob_name']
    )


def delete_old_blob_task(**op_kwargs):
    delete_blob(
        bucket_name=op_kwargs['bucket_name'], 
        blob_name=op_kwargs['blob_name']
    )


start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id= "End", retries=0, dag=dag)

blobs = list_blobs("sixty-analytics",prefix="rtf_")

    
for blob in blobs:
    move_blob = PythonOperator(task_id = "Move_" + blob.name,
                    python_callable = copy_blob_task,
                    op_kwargs = {
                        "bucket_name":watch_bucket, 
                        "blob_name":blob.name, 
                        "new_bucket_name":"rtf_staging", 
                        "new_blob_name":blob.name
                    },
                    dag = dag
                    )

    delete_blob = PythonOperator(task_id = "Delete_" + blob.name,
                    python_callable = delete_old_blob_task,
                    op_kwargs = {
                        "bucket_name":watch_bucket, 
                        "blob_name":blob.name 
                    },
                    dag = dag)
                    
    start_task >> move_blob >> delete_blob >> end_task
