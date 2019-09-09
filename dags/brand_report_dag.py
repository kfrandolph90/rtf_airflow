from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import time
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from essence.analytics.platform import securedcredentials as secure_creds

from RTf.rtf_utils.moat_utils import MoatTile


"""
ToDo:
    - Implement Global DataRange from Context dictionary
    - Implement BQ Upload Task
    - Refactor DCM tile on separate branch

    - Bonus:
        - Build MoatTask subtask (add dest_table and )




"""




## Declare Globals
service_account_email='131786951123-compute@developer.gserviceaccount.com'
credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)

## Load Stuff
credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
moat_token = secure_creds.getDataFromEssenceVault('moat_token')


campaigns = Variable.get('pathToJSON') ## this is GCS path to 



## Define Dag

###### Notes these default args are passed to all tasks pass. start_date, etc dest bucket
args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 8, 1), 
    'email_on_failure': False,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 0, 
    'retry_delay': timedelta(minutes=20),
    'provide_context':True # this makes xcom work
}

dag = DAG('Google Brand Reporting',
            default_args=args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval=None, 
            catchup=False)

def request_and_upload(tile,folder=None):
    filename = moat_tile.get_data(start_date, end_date,moat_token)
    if folder:
        blob_name = folder + "/" + filename
    
    gcs.upload_blob(dest_bucket,blob_name,filename,mode='filename')
    
    gcs_uri = "gs://" + dest_bucket + "/" + blob_name
    
    print("File Upload to {}".format(gcs_uri))
    
    os.remove(filename)
    print("{} Removed Locally".format(filename))
    
    return gcs_uri


def is_active(campaign):
    camp_start_date = datetime.strptime(campaign['startDate'], "%Y-%m-%d")
    camp_end_date = datetime.strptime(campaign['endDate'], "%Y-%m-%d")
    return camp_start_date < datetime.now() < camp_end_date

def build_tasks(campaign):
    moat_tiles = [MoatTile(**tile) for tile in campaign['moatTasks']]
    dcm_reports = [dcmReport(**report) for report in campaign['dcmReports']] ##update this

    moat_start = DummyOperator(task_id="Moat Tasks", retries=0, dag=dag)
    
    moat_tasks =[]
    
    for tile in moat_tiles:
        request_task = PythonOperator(task_id=tile.name + " - Request", python_callable=request_and_upload, op_kwargs={'tile':tile,'folder':'brand-reporting'} ,dag=dag, provide_context=True)
        
        upload_task = PythonOperator(task_id=tile.name + " - BQ Import Task", python_callable= <bq upload task>, dag=dag,provide_context=True)

        moat_tasks.append([moat_start,campaign_task,request_task,upload_task])

    return moat_tasks
"""
    for report in dcm_tiles:
        request_task = PythonOperator(task_id=tile.name + " - Request", python_callable=request_and_upload, op_kwargs={'tile':tile,'folder':'brand-reporting'} ,dag=dag, provide_context=True)
        
        upload_task = PythonOperator(task_id=tile.name + " - BQ Import Task", python_callable= <bq upload task>, dag=dag,provide_context=True)

        moat_tasks.append([moat_start,campaign_task,request_task,upload_task])"""


start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task  = DummyOperator(task_id="End", retries=0, dag=dag)
## build tasks
for campaign in config:    
    if is_active(campaign): ## make sure campaign is active
        campaign_task = DummyOperator(task_id="Start: {}".format(name), retries=0, dag=dag)
        campaign_task >> build_tasks(campaign) >> end_task
            
            
    else:
        logging.info("Campaign: {} Inactive, Remove from config")
        pass










end_task = DummyOperator(task_id= "End", retries=0, dag=dag)
