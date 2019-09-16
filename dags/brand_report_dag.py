from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import time
import os
import json
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from essence.analytics.platform import securedcredentials as secure_creds
from RTF.rtf_utils.moat_utils import MoatTile    
from RTF.rtf_utils.gcp_utils import CloudStorage,BigQuery


args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 9, 1), 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'provide_context':True, # this makes xcom work
    'write_disposition': 'WRITE_EMPTY',
    'create_disposition': 'CREATE_IF_NEEDED',
}

dag = DAG('Google-Brand-Reporting',
            default_args=args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval=None, 
            catchup=False)

## Load Globals
service_account_email='131786951123-compute@developer.gserviceaccount.com'



## Load Queue
with open('/home/airflow/gcs/dags/RTF/brand_report_task_queue.json') as json_file:
    config_json = json.load(json_file)
    campaigns = config_json['campaigns']

"""
ToDo:
    - Implement Global DataRange from Context dictionary
    - Implement BQ Upload Task
    - Refactor DCM tile on separate branch

    - Bonus:
        - Build MoatTask subtask (add dest_table and )




"""

def gcs_to_bq(pull_id,dest_table,mode,ext,**context):
    credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
    bq = BigQuery(credentialsFromVault)

    gcs_uri = context['ti'].xcom_pull(task_ids=pull_id)
    print(gcs_uri)
    
    _ = bq.load_from_gcs('RTF_DWH_Moat',gcs_uri,dest_table,mode,ext)


def request_and_upload(tile,folder=None,**context):
    print(type(tile))
    credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
    moat_token = secure_creds.getDataFromEssenceVault('Moat_Token_Google')
    
    gcs = CloudStorage(credentialsFromVault)
    
    start_date = context['ds_nodash']
    end_date = context['ds_nodash']
    print(start_date)

    filename = tile.get_data(start_date, end_date, moat_token)
    
    if folder:
        blob_name = folder + "/" + filename
    dest_bucket = "rtf_staging"
    
    gcs.upload_blob(dest_bucket,blob_name,filename,mode='filename')
    
    gcs_uri = "gs://" + dest_bucket + "/" + blob_name
    
    print("File Upload to {}".format(gcs_uri))
    
    os.remove(filename)
    print("{} Removed Locally".format(filename))
    
    return gcs_uri


start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task  = DummyOperator(task_id="End", retries=0, dag=dag)

for campaign in campaigns:
    camp_start_date = datetime.strptime(campaign['startDate'], "%Y-%m-%d")
    camp_end_date = datetime.strptime(campaign['endDate'], "%Y-%m-%d")
    if camp_start_date < datetime.now() < camp_end_date:
        print("{} Campaign is Active")
    else:
        pass

    
    ##moat_tiles = [MoatTile(**tile) for tile in ]   
    
    for tile in campaign['moatTasks']:
        
        
        t = MoatTile(tile_id = tile['tile_id'],
                tile_type = tile['tile_type'],
                name = tile['name'],
                level_filters = tile['tile_type'],
                dimensions = tile['dimensions'])

        
        print(type(t))
        task_id_base = campaign['name'] + "_" +  str(t.brandid)
        request_task = PythonOperator(task_id=task_id_base + "_request",python_callable=request_and_upload, op_kwargs={'tile':t,'folder':'Moat_Exports'},dag=dag, provide_context=True)
        
        upload_task = PythonOperator(task_id=task_id_base + "_bqImport", python_callable=gcs_to_bq, op_kwargs={'pull_id':task_id_base + "_request", 'dest_table':t.name,'mode':"Append", 'ext':"json"}, dag=dag,provide_context=True)
        
        start_task >> request_task >> upload_task >> end_task



     

    
