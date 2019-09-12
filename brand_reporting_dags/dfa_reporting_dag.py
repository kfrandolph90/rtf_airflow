from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.models import Variable

from RTF.brand_reporting_dags.dfa_reporting_dag_tasks import dfa_report_extract,dfa_report_load
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 9, 1), 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
    'provide_context':True, # this makes xcom work
    'write_disposition': 'WRITE_EMPTY',
    'create_disposition': 'CREATE_IF_NEEDED',
    'project_id':'essence-analytics-dwh',
    'staging_bucket':'rtf_staging'
}

dag = DAG('Google-Brand-Reporting_CampaignManager',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval=None, 
            catchup=False)

## load tasks
with open('/home/airflow/gcs/dags/RTF/brand_reporting_dags/brand_report_task_queue.json') as json_file:
    config_json = json.load(json_file)
    campaigns = config_json['campaigns']

## build dcm tasks
campaign_manager_tasks = []
for campaign in campaigns:
    campaign_manager_tasks.extend(campaign['dcmTasks'])

print(campaign_manager_tasks)


start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id="End", retries=0, dag=dag)


for task in campaign_manager_tasks:
    report_name = task['name']
    report_id = task['report_id']

    req_id  = str(report_id) + "_request"

    request_task = PythonOperator(task_id=req_id,
                                    python_callable=dfa_report_extract, 
                                    op_args=[report_id],
                                    dag=dag, provide_context=True)
    
    load_id  = str(report_id) + "_load"
    dataset_table = "RTF_DWH_CampaignManager."+report_name
    
    load_bq_task = PythonOperator(task_id=load_id,
                                    python_callable=dfa_report_load, 
                                    op_args=[req_id,dataset_table],
                                    dag=dag, provide_context=True)
    
    
    ##cleaning_tasks = PythonOperator(task_id=task_id_base + "_bqImport", python_callable=gcs_to_bq, op_kwargs={'pull_id':task_id_base + "_request", 'dest_table':t.name,'mode':"Append", 'ext':"json"}, dag=dag,provide_context=True)
    
    start_task >> request_task >> load_bq_task >> end_task


