import logging
from datetime import date, datetime, timedelta

from essence.analytics.platform import securedcredentials as secure_creds
from googleapiclient import discovery

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

logger = logging.getLogger()
logger.setLevel(logging.INFO)


QUERY_START_DATE = datetime(2020, 1, 1, 0, 0)



#FYI
CUSTOMER_ID = '48'  # 'displayName': 'Essence Agency'
LINKED_ID = '13'  # 'google-marketing'
SERVICE_ACCOUNT_EMAIL =  'data-strategy@essence-analytics-dwh.iam.gserviceaccount.com'

# QUERY PARAMS
CAMPAIGN_IDS = [23197607,23219466,22783112,23366227,23213718,23248780,23371034,23204197,23234675,23216235,23539723]
ADVERTISER_IDS = [6071606, 3726053, 9285518, 3853464,6960200, 8709897, 3771812, 4736204, 3404318, 3771812]
PARAMS = {'advertiser_ids': ADVERTISER_IDS, 'campaign_ids': CAMPAIGN_IDS}

## QUERIES - (query_name, query_id,dest. table base)
query_queue = [
    ("RbyF_Consolidated_Weekly", "c8ebeeebf73e4250b932905ed0860f4d",
     "RTF_RbyF_Consolidated_Weekly"),
    ("RbyF_Consolidated_Weekly_RDID", "93656e5123994738b24285b5e4b98ac7",
     "RTF_RbyF_Consolidated_Weekly_RDID"),
    ("RbyF_Consolidated_Summary", "6418a5a86cc14456bdd4930ef6faa4a9",
     "RTF_RbyF_Consolidated_Summary"),
    ("RbyF_Consolidated_Summary_RDID", "1e17e9499ad14554b1e2546b57e8ac78",
     "RTF_RbyF_Consolidated_Summary_RDID")
]








def build_service(service_account_email):
    # API Details
    API_NAME = 'adsdatahub'
    API_VERSION = 'v1'
    API_SCOPES = ['https://www.googleapis.com/auth/adsdatahub']

    # Discovery URL
    _FCQ_SERVICE = 'adsdatahub.googleapis.com'
    _FCQ_VERSION = 'v1'
    _REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
    _DISCOVERY_URL_TEMPLATE = 'https://%s/$discovery/rest?version=%s&key=%s'
    DEVELOPER_KEY = 'AIzaSyAnc0kt2Gsi7IgceRqOX5w6cGi_XGKL_uw'  # hide this somehow

    discovery_url = _DISCOVERY_URL_TEMPLATE % (
        _FCQ_SERVICE, _FCQ_VERSION, DEVELOPER_KEY)
    credentialsFromVault = secure_creds.getCredentialsFromEssenceVault(
        service_account_email)
    credentialsFromVault = credentialsFromVault.with_scopes(API_SCOPES)
    service = discovery.build(API_NAME, API_VERSION, credentials=credentialsFromVault,
                              discoveryServiceUrl=discovery_url,
                              cache_discovery=False)
    return service


def format_params(params):
    formated_query_parameters = {}
    for param_name, param_values in params.items():
        if type(param_values) == list:
            vals = [{'value': str(val)} for val in param_values]
            formated_query_parameters[param_name] = {
                'arrayValue': {'values': vals}}
        else:
            formated_query_parameters[param_name] = {'value': param_values}
    return formated_query_parameters


def build_analysis_body(start_date, end_date, dest_table, linked_ads_id, customer_id, time_zone='America/New_York', **params):
    analysis_body = {
        'spec': {
            'adsDataCustomerId': linked_ads_id,
            'matchDataCustomerId': customer_id,
            'startDate': {'year': start_date.year,
                          'month': start_date.month,
                          'day': start_date.day},
            'endDate': {'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day},
            'time_zone': time_zone
        },
        'destTable': dest_table,
        'customerId': customer_id}

    if params:
        logger.info("Analysis Body has Params")
        logger.info(params)
        analysis_body['spec']['parameterValues'] = format_params(params)

    return analysis_body


def run_adh(query_id, dest_table, params=None, **kwargs):
    dest_table = dest_table + "_" + kwargs['yesterday_ds_nodash']
    logger.debug(dest_table)
    
    start_date = QUERY_START_DATE
    end_date = datetime.strptime(kwargs['yesterday_ds_nodash'], '%Y%m%d')    
    
    service = build_service(SERVICE_ACCOUNT_EMAIL)
    
    query_body = build_analysis_body(
        start_date, end_date, dest_table, LINKED_ID, CUSTOMER_ID, **params)
    
    logger.debug(query_body)

    query_name = 'customers/{}/analysisQueries/{}'.format(
        customer_id, query_id)

    logger.info("Run {} w/ {}".format(query_name,query_body))
    
    response = service.customers().analysisQueries().start(
        name=query_name, body=query_body).execute()

    operation = response['name']
    
    logger.info("Operation {}".format(operation))

    return dest_table


## DAG Junk ###
def_args = {
    'owner': 'RTF',
    'depends_on_past': False,
    'task_concurrency': 3,
    'retry_delay': timedelta(minutes=20),
    'retries':4
}

dag = DAG('BR_Reporting_ADH_DAG',
          description='Runs ADH R/F queries for Google Brand Reporting',
          default_args=def_args,
          # schedule_interval='0 10 * * *',  # CHANGE: to every monday for 1-1-20 to Date
          schedule_interval='@once',
          start_date=datetime(2020, 1, 20, 0, 0),
          catchup=False)

query_queue = [
    ("RbyF_Consolidated_Weekly", "c8ebeeebf73e4250b932905ed0860f4d",
     "RTF_RbyF_Consolidated_Weekly"),
    ("RbyF_Consolidated_Weekly_RDID", "93656e5123994738b24285b5e4b98ac7",
     "RTF_RbyF_Consolidated_Weekly_RDID"),
    ("RbyF_Consolidated_Summary", "6418a5a86cc14456bdd4930ef6faa4a9",
     "RTF_RbyF_Consolidated_Summary"),
    ("RbyF_Consolidated_Summary_RDID", "1e17e9499ad14554b1e2546b57e8ac78",
     "RTF_RbyF_Consolidated_Summary_RDID")
]


with dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')

    for query in query_queue:

        query_name, query_id, dest_table_base = query

        logger.info("BUILD TASKS: {}".format(query_name))

        adh_run_task = PythonOperator(task_id=query_name,
                                      python_callable=run_adh,
                                      op_args=[query_id, dest_table_base],
                                      op_kwargs={'params':PARAMS},
                                      provide_context=True)  # context allows python to access context dictionary

        adh_results_sensor = BigQueryTableSensor(task_id=query_name + "-Table_Check",
                                                 project_id='essence-ads-data-hub',
                                                 dataset_id='full_circle_shared',
                                                 table_id="{{{{ ti.xcom_pull(task_ids='{}') }}}}".format(query_name),
                                                 poke_interval=60*30,  # 30min
                                                 bigquery_conn_id = 'bigquery_default',
                                                 mode='poke',
                                                 timeout=60 * 60 * 3,
                                                 )

        if "Summary" in query:

            BigQueryTableSensor(task_id=query_name + "-Table_Check",
                                                 project_id='essence-ads-data-hub',
                                                 dataset_id='full_circle_shared',
                                                 table_id="{{{{ ti.xcom_pull(task_ids='{}') }}}}".format(query_name),
                                                 poke_interval=60*30,  # 30min
                                                 bigquery_conn_id = 'bigquery_default',
                                                 mode='poke',
                                                 timeout=60 * 60 * 3,
                                                 )


        
        
        start_task.set_downstream(adh_run_task)
        adh_run_task.set_downstream(adh_results_sensor)
        adh_results_sensor.set_downstream(end_task)
