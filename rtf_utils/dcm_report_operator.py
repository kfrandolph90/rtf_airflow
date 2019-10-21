from RTF.rtf_utils.dfa_utils import get_dfa_report
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException



class DCM_to_GCS(BaseOperator):

    '''
    Calls DCM API endpoint to update and run report
    API docs:https://api.moat.com/pubdocs

    :param brand_id: id of Moat tile
    :type brand_id: int

    :param s: request start date YYYYMMDD
    :type: str 

    :param e: request end date (inclusive) YYYYMMDD
    :type: str 

    :param level_filter: (optional) filter of form {'level#':id}
    :type level_filter: dict

    :param dimensions: (optional) list of desired dimensions
    :type dimensions: list

    :param bucket: destination GCS bucket to save data in
    :type bucket: str

    :param prefix: prefix to give GCS blob (equivalent to folder)
    :type prefix: str

    :param suffix: suffix to give GCS blob
    :type prefix: str

    '''

    template_fields = ('s', 'e','suffix')
    
    ui_color = '##285F4'

    @apply_defaults
    def __init__(
            self,
            brand_id,
            s,
            e,
            level_filter=None,
            dimensions=None,
            bucket='rtf_staging',
            prefix='moat/',
            suffix=None,
            *args, **kwargs):

        super().__init__(*args, **kwargs)
        
        # Moat Config
        self.brand_id = brand_id
        self.s = s
        self.e = e
        self.level_filter = level_filter
        self.dimensions = dimensions

        # GCS Config
        self.bucket = bucket
        self.prefix = prefix
        self.suffix = suffix

    def execute(self, context):
        # build moat tile
        logging.info('Instantiate Moat Tile')
        
        
        tile = MoatTile(self.brand_id,self.level_filter,self.dimensions)

        token = Variable.get ('rtf_moat_token')
        ## somehow get token in a better way

        logging.info('Fetch Token')

        if self.level_filter:
            filter_id = [*self.level_filter.values()][0]

        time.sleep(random.randint(1,5)) # fuck shit up

        filename = tile.get_data(self.s,self.e, token)

        if filename:
            logging.info('Response Saved Locally @ {}'.format(filename))

        else:
            logging.error('No Response')
            raise AirflowSkipException()

        file_tokens = [self.brand_id,filter_id,self.suffix]

        blob_name = "_".join([str(x) for x in file_tokens if x ])   ## PRETTY CLEVER KYLE

        if self.prefix:
            blob_name = str(self.prefix) + blob_name + '.json'

        hook = GoogleCloudStorageHook()
        
        hook.upload(
            bucket = self.bucket,
            object = blob_name,          
            filename = filename
        ) ## docs don't match repo
        
        logging.info("{} uploaded to {}".format(blob_name,self.bucket))
        os.remove(filename)
        
        logging.info("{} deleted from local".format(filename))

        return (self.bucket,blob_name) ## should get pushed to xcom if do_xcom_push is set to True in baseclass



'''

for i,task in enumerate(tasks):
    brand_id, level_filter, dimensions  = task

    
    
    Moat_To_GCS(task_id = moat_i,
    
        brand_id,
        start_date = '{{ds}}',
        end_date = ' {{-7}}',
        level_filter =
        dimensions = 
        prefix = brand_reporting
        suffix = {{ds}})

    GCSToBQ

    GCS_To_BQ(task_id)
'''

