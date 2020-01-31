from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.utils.helpers import chain
from datetime import timedelta, datetime
from essence.looker.wrappers.datalab import DataLab
import re


PROJECT_ID = "essence-analytics-dwg"
DEST_DATASET = "RTF_DL_Mirror."

CROSS_CHANNEL_FACT_ID = 19027
CROSS_CHANNEL_FACT_TABLE = DEST_DATASET + "cross_channel_FACT"

CROSS_CHANNEL_FACT_MANUAL_UPLOAD_ID = 19028
CROSS_CHANNEL_FACT_MANUAL_UPLOAD_TABLE = DEST_DATASET + "cross_channel_FACT_manual_upload"

TASK_QUEUE = [
    ('cross_channel_fact',CROSS_CHANNEL_FACT_ID,CROSS_CHANNEL_FACT_TABLE),
    ('cross_channel_fact_manual_upload', CROSS_CHANNEL_FACT_MANUAL_UPLOAD_ID,CROSS_CHANNEL_FACT_MANUAL_UPLOAD_TABLE)
    ]


def look_to_bq(look_id,table,if_exists='replace'):
    with DataLab.from_vault("LookerAPICreds") as dl:
        df = dl.look_to_df(look_id,limit=100000000)
        
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns=lambda x: re.sub('[^a-zA-Z0-9]', '_', x), inplace=True)
        df.rename(columns=lambda x: re.sub('__*', '_', x), inplace=True)
        df.columns = [x[1:] if x.startswith('_') else x for x in df.columns]        
        df.columns = [x[:-1] if x.endswith('_') else x for x in df.columns]
        df.columns = [x.lower() for x in df.columns]

        df.to_gbq(destination_table=table,project_id=PROJECT_ID,if_exists=if_exists)


dag_args = {
    'owner': 'RTF',
    'depends_on_past': False,
    'execution_timeout': timedelta(minutes=50),
    'task_concurrency': 1,
    'schedule_interval': '@once',
    "retries":2,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
        dag_id = "rtf_datalab_replica",
        description="for all your datalab replica needs",
        default_args=dag_args,start_date=datetime(2020,1,21)
        ) as dag:

    start_dummy_task = DummyOperator(task_id='start')
    end_dummy_task = DummyOperator(task_id='end')

    for task in TASK_QUEUE:
        task_name, look_id, dest_table = task
        datalab_task = PythonOperator(
                task_id =  task_name,
                python_callable = look_to_bq,
                op_kwargs={
                    "look_id":look_id,
                    "table":dest_table
                }
            )
        
        start_dummy_task.set_downstream(datalab_task)
        datalab_task.set_downstream(end_dummy_task)