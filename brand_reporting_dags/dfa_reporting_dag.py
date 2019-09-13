from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.models import Variable

from RTF.brand_reporting_dags.dfa_reporting_dag_tasks import dfa_report_extract,dfa_report_load,clean_up
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 9, 1), 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
    'provide_context':True,# this makes xcom work
    'catchup':False
}

dag = DAG('Google-Brand-Reporting_CampaignManager',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval='0 6 * * *', 
            catchup=False)

## load tasks
with open('/home/airflow/gcs/dags/RTF/brand_reporting_dags/brand_report_task_queue.json') as json_file:
    config_json = json.load(json_file)
    campaigns = config_json['campaigns']

## build dcm tasks
start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id="End", retries=0, dag=dag)

for i,campaign in enumerate(campaigns):

    campaign_task = DummyOperator(task_id="Campaign_{}".format(i+1), retries=0, dag=dag)
    
    campaign_manager_tasks = campaign['dcmTasks']
    start_task.set_downstream(campaign_task)

    for task in campaign_manager_tasks:
        report_name = task['name']
        report_id = task['reportId']

        task_id_base = "Report-" + str(report_id)
        
        req_id  = task_id_base + "-extract"

        request_task = PythonOperator(task_id=req_id,
                                        python_callable=dfa_report_extract, 
                                        op_args=[report_id],
                                        dag=dag, provide_context=True)
        
        load_id  = task_id_base + "-load"
        
       
        if task.get('dest'):
            dest = task['dest']

        else:
            dest = ("RTF_DWH_CampaignReporting." + 
            task['name'] + "_" +
            datetime.now().strftime('%Y%m%d'))
        
        load_bq_task = PythonOperator(task_id=load_id,
                                        python_callable=dfa_report_load, 
                                        op_args=[req_id,dest],
                                        dag=dag, provide_context=True)


        
        cleaning_tasks = PythonOperator(task_id=task_id_base + "_clean", python_callable=clean_up, op_args=[req_id],dag=dag,provide_context=True)
        
        campaign_task >> request_task >> load_bq_task >> cleaning_tasks

        end_task.set_upstream(cleaning_tasks)

        