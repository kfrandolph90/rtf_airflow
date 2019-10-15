from airflow.operators.dummy_operator import DummyOperator

from RTF.rtf_utils.moat_operator import Moat_To_GCS

from airflow import DAG

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
}

dag = DAG('Google-Brand-Reporting_CampaignManager',
            default_args=default_args,
            description='Move DataLab Export to Staging GCS',
            schedule_interval='@once', 
            catchup=False)

dag.doc_md = "### Let's test out a custom DIY operator"

start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id="End", retries=0, dag=dag)

moat_request_task = Moat_To_GCS(brand_id = 6179366, 
                                start_date = '{{yesterday_ds_nodash}}',
                                end_date = '{{yesterday_ds_nodash}}',
                                level_filter = {'level2':2604566125},
                                dimensions = ['date','level4'],
                                bucket = 'rtf_staging',
                                prefix = 'moat_test/',
                                suffix = '{{ ds_nodash }}'
                                )

start_task > moat_request_task > end_task