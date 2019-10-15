from moat_utils import MoatTile
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable




class Moat_To_GCS(BaseOperator):

    '''
    Calls Moat API endpoint and stores result to GCS bucket specified.
    API docs:https://api.moat.com/pubdocs

    :param brand_id: id of Moat tile
    :type brand_id: int

    :param start_date: request start date YYYYMMDD
    :type: str 

    :param end_date: request end date (inclusive) YYYYMMDD
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

    template_fields = ('start_date', 'end_date','suffix')
    
    ui_color = '#EF8620'

    @apply_defaults
    def __init__(
            self,
            brand_id,
            start_date,
            end_date,
            level_filter=None,
            dimensions=None,
            bucket='rtf_staging',
            prefix='moat/',
            suffix=None,
            *args, **kwargs):

        super().__init__(*args, **kwargs)
        
        # Moat Config
        self.brand_id = brand_id
        self.start_date = start_date
        self.end_date = end_date
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


        logging.info('Fetch Token')
        token = Variable.get ('rtf_moat_token')
        ## somehow get token

        logging.info('Fetch Token')
        
        filename = tile.get_data(self.start_date,self.end_date, token)

        if filename:
            logging.info('Response Saved Locally @ {}'.format(filename))

        else:
            logging.error('No Response')
            return

        if self.filter:
            filter_id = [*self.filter.values()][0] ## this probably bad
            blob_name = self.brand_id + '_' + filter_id + '_' + self.suffix + '.json'
        else:
            blob_name = self.brand_id + '_' + self.suffix + '.json'
        
        hook = GoogleCloudStorageHook()
        
        hook.upload(
            bucket_name = self.bucket,
            object_name = blob_name,          
            filename = self.filename
        )
        
        logging.info("{} uploaded to {}".format)

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



        
        

