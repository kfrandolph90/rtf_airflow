import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ADHOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            service_account_email,
            linked_ads_id, 
            customer_id, 
            query_id,
            dest_table,
            since,
            until,
            time_zone=None,        
            params=None,
            *args, 
            **kwargs)


        super().__init__(*args, **kwargs)
        self.service_account_email = service_account_email
        self.linked_ads_id = linked_ads_id
        self.customer_id = customer_id
        self.query_id = query_id        
        self.dest_table = dest_table
        self.since  = since
        self.until = until

    def build_spec_body(self):
        self.spec_body = {
        'spec': {
            'adsDataCustomerId': self.linked_ads_id,
            'matchDataCustomerId': self.customer_id,
            'startDate': {'year': self.since.year,
                          'month': self.since.month,
                          'day': self.since.day},
            'endDate': {'year': self.until.year,
                        'month': self.until.month,
                        'day':self.until.day}
        },
        'destTable': self.dest_table,
        'customerId': self.customer_id}

        if self.time_zone:
            self.spec_body['spec']['time_zone'] = self.time_zone

        if self.params:
            logger.debug("Analysis Body has Params")
            logger.debug(self.params)
            self.spec_body['spec']['parameterValues'] = self.format_params()

    def format_params(self):
        formated_query_parameters = {}
        for param_name, param_values in self.params.items():
            if type(param_values) == list:
                vals = [{'value': str(val)} for val in param_values]
                formated_query_parameters[param_name] = {
                    'arrayValue': {'values': vals}}
            else:
                formated_query_parameters[param_name] = {'value': param_values}
        return formated_query_parameters
    

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


    def execute(self, context):
        service = build_service(service_account_email)
        query_name = 'customers/{}/analysisQueries/{}'.format(self.customer_id, self.query_id)

        response = service.customers().analysisQueries().start(name=query_name, body=query_body).execute()

        operation = response['name']