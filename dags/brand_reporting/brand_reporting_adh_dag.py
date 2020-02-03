import logging
from datetime import date, datetime, timedelta

from essence.analytics.platform import securedcredentials as secure_creds
from googleapiclient import discovery

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

logger = logging.getLogger()
logger.setLevel(logging.INFO)


QUERY_START_DATE = datetime(2020, 1, 1, 0, 0)
CUSTOMER_ID = '48'  # 'displayName': 'Essence Agency'
LINKED_ID = '13'  # 'google-marketing'
SERVICE_ACCOUNT_EMAIL = 'data-strategy@essence-analytics-dwh.iam.gserviceaccount.com'


# ADH QUERIES
WEEKLY_RF_QUERY = {'name': 'RbyF_Consolidated_Weekly',
                   'query_id': 'c8ebeeebf73e4250b932905ed0860f4d', 'adh_dest_table': 'RTF_RbyF_Consolidated_Weekly'}
WEEKLY_RF_QUERY_RDID = {'name': 'RbyF_Consolidated_Weekly_RDID',
                        'query_id': '93656e5123994738b24285b5e4b98ac7', 'adh_dest_table': 'RTF_RbyF_Consolidated_Weekly_RDID'}
SUMMARY_RF_QUERY = {'name': 'RbyF_Consolidated_Summary',
                    'query_id': '6418a5a86cc14456bdd4930ef6faa4a9', 'adh_dest_table': 'RTF_RbyF_Consolidated_Summary'}
SUMMARY_RF_QUERY_RDID = {'name': 'RbyF_Consolidated_Summary_RDID',
                         'query_id': '1e17e9499ad14554b1e2546b57e8ac78', 'adh_dest_table': 'RTF_RbyF_Consolidated_Summary_RDID'}

# QUERY PARAMS
CAMPAIGN_IDS = [23197607, 23219466, 22783112, 23366227, 23213718,
                23248780, 23371034, 23204197, 23234675, 23216235, 23539723]
ADVERTISER_IDS = [6071606, 3726053, 9285518, 3853464,
                  6960200, 8709897, 3771812, 4736204, 3404318, 3771812]
ADH_PARAMS = {'advertiser_ids': ADVERTISER_IDS, 'campaign_ids': CAMPAIGN_IDS}

# DESTINATION TABLES
WEEKLY_RF_DEST_TABLE = "002_ADH_RF_Weekly"
SUMMARY_RF_TABLE = "002_ADH_RF_SUMMARY"


query_queue = [WEEKLY_RF_QUERY,
               WEEKLY_RF_QUERY_RDID,
               SUMMARY_RF_QUERY,
               SUMMARY_RF_QUERY_RDID]


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
        logger.debug('Parameters for analysis query detected')
        logger.debug(params)
        analysis_body['spec']['parameterValues'] = format_params(params)

    return analysis_body


def run_adh(query_id, dest_table, params=None, **kwargs):
    logger.debug(dest_table)

    start_date = QUERY_START_DATE
    end_date = datetime.strptime(kwargs['yesterday_ds_nodash'], '%Y%m%d')

    service = build_service(SERVICE_ACCOUNT_EMAIL)

    query_body = build_analysis_body(
        start_date, end_date, dest_table, LINKED_ID, CUSTOMER_ID, **params)

    logger.debug(query_body)

    query_name = 'customers/{}/analysisQueries/{}'.format(
        CUSTOMER_ID, query_id)

    logger.info('Run {} w/ {}'.format(query_name, query_body))

    response = service.customers().analysisQueries().start(
        name=query_name, body=query_body).execute()

    operation = response['name']

    logger.info('Operation {}'.format(operation))

    return dest_table


## DAG Junk ###
def_args = {
    'owner': 'RTF',
    'depends_on_past': False,
    'task_concurrency': 3,
    'retry_delay': timedelta(minutes=5),
    'retries': 4,
    'params': {
        'adh_project_id': 'essence-ads-data-hub',
        'adh_dataset': 'full_circle_shared',
        'dest_project_id': 'essence-analytics-dwh',
        'dest_dataset': 'rtf_br_reporting'
    },
    'bigquery_conn_id': 'bigquery_default'
}

dag = DAG('BR_Reporting_ADH_DAG',
          description='Runs ADH R/F queries for Google Brand Reporting',
          default_args=def_args,
          schedule_interval='5 13 * * 1', # At 13:05 on Monday (8am UTC)
          start_date=datetime(2020, 2, 2, 0, 0),
          catchup=False,
          template_searchpath="/home/airflow/gcs/dags/RTF/brand_reporting/sql/"
          )

with dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')

    merge_wkly = BigQueryOperator(
        sql='RF_WEEKLY.sql',
        task_id='MERGE_weekly_tables',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        params={
            'main_table': WEEKLY_RF_QUERY['adh_dest_table'],
            'join_table': WEEKLY_RF_QUERY_RDID['adh_dest_table']
        },
        destination_dataset_table='{{ params.dest_project_id }}:{{ params.dest_dataset }}.' + WEEKLY_RF_DEST_TABLE)

    merge_summary = BigQueryOperator(
        sql='RF_SUMMARY.sql',
        task_id='MERGE_summary_tables',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        params={
            'main_table': SUMMARY_RF_QUERY['adh_dest_table'],
            'join_table': SUMMARY_RF_QUERY_RDID['adh_dest_table']
        },
        destination_dataset_table='{{ params.dest_project_id }}:{{ params.dest_dataset }}.' + SUMMARY_RF_TABLE)

    for query in query_queue:

        query_name, query_id, dest_table = query['name'], query['query_id'], query['adh_dest_table']

        delete_task = BigQueryTableDeleteOperator(task_id='CLEAN_{}'.format(dest_table),
                                                  deletion_dataset_table='{{ params.adh_project_id }}:{{ params.adh_dataset}}.' + dest_table,
                                                  ignore_if_missing=True)

        adh_run_task = PythonOperator(task_id='RUN_{}'.format(query_name),
                                      python_callable=run_adh,
                                      op_args=[query_id, dest_table],
                                      op_kwargs={'params': ADH_PARAMS},
                                      provide_context=True)  # context allows python to access context dictionary

        adh_results_sensor = BigQueryTableSensor(task_id='CHECK_{}'.format(dest_table),
                                                 project_id='{{ params.adh_project_id }}',
                                                 dataset_id='{{ params.adh_dataset }}',
                                                 table_id=dest_table,
                                                 poke_interval=60*20,  # 30min
                                                 mode='poke',
                                                 timeout=60 * 60 * 3,  # 3 hours
                                                 )

        start_task >> delete_task >> adh_run_task >> adh_results_sensor

        if 'Summary' in query_name:
            merge_summary.set_upstream(adh_results_sensor)
        else:
            merge_wkly.set_upstream(adh_results_sensor)

    merge_summary >> end_task
    merge_wkly >> end_task