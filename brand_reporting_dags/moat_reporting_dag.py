from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.models import Variable

from RTF.brand_reporting_dags.moat_reporting_dag_tasks import moat_report_extract,load_bq,clean_up
from datetime import datetime, timedelta
import json


## DAG Instantiation Stuff ##
default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 9, 1), 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
    'provide_context':True,# this makes xcom work
    'catchup':False}

dag = DAG('Google-Brand-Reporting_CampaignManager',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval='0 6 * * *', 
            catchup=False)

## Declare Destinations ##
dest_bucket = 'rtf_staging'
dest_dataset = 'rtf_dev'


## Tasks ## 
tiles = [
    {'tile_id':13120,'filters':[{'level1':10154328017481183}],'dimensions':['date','level1','level4']},
    {'tile_id':13386,'filters':[{'level1':7020493427}],'dimensions':['date','level1','level4']}

]


## Dynamically Generate DAG
start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id="End", retries=0, dag=dag)

for tile in tiles:
    tile_id = tile['tile_id']
    folder = "moat/{}".format(tile_id)
    file_uri = "gs://" + folder + "/*"
    table = "moat_" + str(tile_id)
    
    reqs = []
    for i,req in enumerate(tile['filters']):

        req_task = PythonOperator(task_id="req_{}_{}".format(tile_id,i),
                                    python_callable=moat_report_extract, 
                                    op_args=[tile_id,dest_bucket,folder],
                                    dag=dag, provide_context=True)
        reqs.append(req_task)
    
    bq_task = PythonOperator(task_id="load_{}".format(tile_id),
                                python_callable=load_bq, 
                                op_args=[tile_id,file_uri,table],
                                dag=dag, provide_context=True)
    
    clean_task = PythonOperator(task_id="clean_{}".format(tile_id))
    
    start_task >> reqs >> bq_task >> clean_task >> end_task

