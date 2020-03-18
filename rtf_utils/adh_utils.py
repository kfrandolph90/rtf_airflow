from essence.analytics.platform import securedcredentials as secure_creds
from googleapiclient import discovery
import logging

# DEVELOPER_KEY = 'AIzaSyAnc0kt2Gsi7IgceRqOX5w6cGi_XGKL_uw'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ADH:
    def __init__(self,service_account_email,developer_key):
        self.service_account_email = service_account_email
        self.developer_key = developer_key

    def build_service(self):
        # API Details
        API_NAME = 'adsdatahub'
        API_VERSION = 'v1'
        API_SCOPES = ['https://www.googleapis.com/auth/adsdatahub']

        # Discovery URL
        _FCQ_SERVICE = 'adsdatahub.googleapis.com'
        _FCQ_VERSION = 'v1'
        _REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
        _DISCOVERY_URL_TEMPLATE = 'https://%s/$discovery/rest?version=%s&key=%s'
        DEVELOPER_KEY = self.developer_key

        discovery_url = _DISCOVERY_URL_TEMPLATE % (
            _FCQ_SERVICE, _FCQ_VERSION, DEVELOPER_KEY)
        credentialsFromVault = secure_creds.getCredentialsFromEssenceVault(
            self.service_account_email)
        credentialsFromVault = credentialsFromVault.with_scopes(API_SCOPES)
        service = discovery.build(API_NAME, API_VERSION, credentials=credentialsFromVault,
                                discoveryServiceUrl=discovery_url,
                                cache_discovery=False)
        return service


    def format_params(self,params):
        formated_query_parameters = {}
        for param_name, param_values in params.items():
            if type(param_values) == list:
                vals = [{'value': str(val)} for val in param_values]
                formated_query_parameters[param_name] = {
                    'arrayValue': {'values': vals}}
            else:
                formated_query_parameters[param_name] = {'value': param_values}
        return formated_query_parameters


    def build_analysis_body(self, start_date, end_date, dest_table, linked_ads_id, customer_id, time_zone='America/New_York', **params):
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
            analysis_body['spec']['parameterValues'] = self.format_params(params)

        return analysis_body


    def run_adh(self,query_id, start_date,end_date,dest_table,linked_ads_id,customer_id,params=None, **kwargs):
        
        service = self.build_service()

        query_body = self.build_analysis_body(
            start_date, 
            end_date, 
            dest_table, 
            linked_ads_id, 
            customer_id, **params)

        logger.debug(query_body)

        query_name = 'customers/{}/analysisQueries/{}'.format(
            customer_id, query_id)

        logger.info('Run {} w/ {}'.format(query_name, query_body))

        response = service.customers().analysisQueries().start(
            name=query_name, body=query_body).execute()

        operation = response['name']

        logger.info('Operation {}'.format(operation))

        return dest_table