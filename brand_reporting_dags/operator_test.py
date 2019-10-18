"""
#Moat DAG for Google Brand Reporting

##Team/Owner:
RTF - Kyle Randolph - kyle.randolph@essenceglobal.com

##Details:
- Dag will dynamically run a series of Moat API requests to build dataset need for brand reporting initiative.
- Uses GCS hook to upload API response to folder w/ GC
    - Filename convention: `<moat tile>/<moat tile id>_<filter ID>_<date pulled>`
- All files from a given tile are loaded into BQ (with appropriate schema) with the date of the pull
"""
## Base Lib imports
import json
import logging
from datetime import datetime, timedelta

## Import Airflow operators and models
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

## Import RTF utils
from RTF.rtf_utils.moat_operator import Moat_To_GCS
from RTF.rtf_utils.moat_utils import moat_schemas

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 10, 1), 
    'retries': 3, 
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5),
    'provide_context':True, # this makes xcom work
    'retry_exponential_backoff':True    
}

dag = DAG('RTF_BrandReporting_Moat_DAG',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval='@once', 
            catchup=False,
            concurrency=2,
            )

dag.doc_md = __doc__

tasks = Variable.get('moat_tasks', deserialize_json=True)
logging.info("Loaded Tasks")

for tile, campaigns in tasks.items():
    tile = int(tile)  ##TODO: tile type on variable load
    start_task = DummyOperator(task_id='Start_{}'.format(tile), retries=0, dag=dag)
    
    store_task = GoogleCloudStorageToBigQueryOperator(task_id="Store_{}".format(tile),
                                                bucket= 'rtf_staging',
                                                source_objects = ['{}/*'.format(tile)],
                                                destination_project_dataset_table = 'essence-analytics-dwh:RTF_DWH_Moat.{}_'.format(tile) + '{{ ds_nodash }}',
                                                schema_fields = moat_schemas.get(tile),
                                                source_format = 'NEWLINE_DELIMITED_JSON',
                                                create_disposition = 'CREATE_IF_NEEDED',
                                                write_disposition = 'WRITE_TRUNCATE',
                                                autodetect = False,
                                                dag=dag)
    
    for campaign in campaigns:
        level_filter, dimensions = campaign #unpack tuple
        
        filter_value = [*level_filter.values()][0]

        request_task = Moat_To_GCS(task_id= "Req_{}_{}".format(tile,filter_value),
                                brand_id = tile, 
                                bucket= 'rtf_staging',
                                s = '{{yesterday_ds_nodash}}',
                                e = '{{yesterday_ds_nodash}}',
                                level_filter = level_filter,
                                dimensions = dimensions,
                                prefix = '{}/'.format(tile),
                                suffix = '{{ ds_nodash }}',
                                dag=dag,
                               
                                )
        
        start_task.set_downstream(request_task)
        request_task.set_downstream(store_task)