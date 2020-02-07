from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.utils.helpers import chain
from datetime import timedelta, datetime
from essence.looker.wrappers.datalab import DataLab
import re

"""
TODO:
- Create branch operator
    - If table exists run merge query. if not create
    - Remove time component from tablename so it will overwrite
        - Maybe even delete


"""

logger = logging.getLogger()


CROSS_CHANNEL_FACT_ID = 19027
CROSS_CHANNEL_FACT_TABLE ="003_cross_channel_FACT"

CROSS_CHANNEL_FACT_MANUAL_UPLOAD_ID = 19028
CROSS_CHANNEL_FACT_MANUAL_UPLOAD_TABLE = DEST_DATASET + "003_cross_channel_FACT_manual_upload"

TASK_QUEUE = [
    ('cross_channel_fact',CROSS_CHANNEL_FACT_ID,CROSS_CHANNEL_FACT_TABLE),
    ('cross_channel_fact_manual_upload', CROSS_CHANNEL_FACT_MANUAL_UPLOAD_ID,CROSS_CHANNEL_FACT_MANUAL_UPLOAD_TABLE)
    ]


SCHEMA = [
{'name':'time_1_date_of_activity','type':'DATE'},
{'name':'olive_campaign_id','type':'INTEGER'},
{'name':'olive_plan_id','type':'INTEGER'},
{'name':'olive_plan_line_id','type':'INTEGER'},
{'name':'olive_placement_id','type':'INTEGER'},
{'name':'performance_and_delivery_1_impressions','type':'FLOAT'},
{'name':'performance_and_delivery_2_clicks','type':'FLOAT'},
{'name':'performance_and_delivery_spend_in_usd','type':'FLOAT'},
{'name':'performance_and_delivery_spend_in_media_plan_currency','type':'FLOAT'},
{'name':'performance_and_delivery_spend_in_client_currency','type':'FLOAT'},
{'name':'performance_and_delivery_spend_in_buying_currency','type':'FLOAT'}
]

def clean_crud(df):
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns=lambda x: re.sub('[^a-zA-Z0-9]', '_', x), inplace=True)
    df.rename(columns=lambda x: re.sub('__*', '_', x), inplace=True)
    df.columns = [x[1:] if x.startswith('_') else x for x in df.columns]        
    df.columns = [x[:-1] if x.endswith('_') else x for x in df.columns]
    df.columns = [x.lower() for x in df.columns]
    return df


def query_looker(look_id,dest_table, days_prior=10, **kwargs):
    params = kwargs['params']


    
    dest_table = dest_table + "_" + kwargs['ds_nodash'] # table_YYYYMMDD

    # build date range
    end_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    start_date = end_date - timedelta(days=days_prior)

    start_date, end_date = start_date.strftime('%Y-%m-%d'), start_date.strftime('%Y-%m-%d')

    # build look update body w/ date filters

    look_settings = {
        'id':0,
        'runtime':0,
        'client_id': None,
        'filters':{
            'calendar.calendar_date':'{} to {}'.format(start_date,end_date)  
        },
        'dynamic_fields': None
    }

    ## login to dl
    with DataLab.from_vault("LookerAPICreds") as dl:
        logger.info("UPDATING Look {}".format(look_id))
        dl.update_look(look_id=look_id, update_fields= look_settings, verbose=False)
        
        logger.info("DOWNLOADING Look {}".format(look_id))
        
        df = dl.look_to_df(look_id,limit=1000000)
        
        logger.info("CLEANING Look {}".format(look_id))
        df = clean_crud(df)
        
        df.to_gbq(destination_table=dest_table,project_id=params['dest_project_id'],table_schema=SCHEMA,if_exists='replace')
    

dag_args = {
    'owner': 'RTF',
    'depends_on_past': False,
    'execution_timeout': timedelta(minutes=50),
    'task_concurrency': 1,
    #'schedule_interval': '@once',
    'schedule_interval':None,
    "retries":4,
    "retry_delay": timedelta(minutes=10),
    "params":{
        'dest_project_id': 'essence-analytics-dwh', 
        'dest_dataset': 'RTF_DL_Mirror'

    },
    'bigquery_conn_id': 'bigquery_default'
}

with DAG(
        dag_id = "rtf_datalab_replica",
        description="for all your datalab replica needs",
        default_args=dag_args,start_date=datetime(2020,1,5)
        ) as dag:

    start_dummy_task = DummyOperator(task_id='start')
    end_dummy_task = DummyOperator(task_id='end')

    for task in TASK_QUEUE:
        task_name, look_id, dest_table = task
        datalab_task = PythonOperator(
                task_id =  task_name,
                python_callable = query_looker,
                op_kwargs={
                    "look_id":look_id,
                    "table":dest_table
                }
            )
        
        start_dummy_task.set_downstream(datalab_task)
        datalab_task.set_downstream(end_dummy_task)