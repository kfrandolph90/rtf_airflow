import json
import os
from datetime import datetime,timedelta

from essence.analytics.platform import securedcredentials as secure_creds


try:
    from RTF.rtf_utils.moat_utils import MoatTile
    from RTF.rtf_utils.gcp_utils import BigQuery, CloudStorage
except:
    from rtf_utils.moat_utils import MoatTile
    from rtf_utils.gcp_utils import BigQuery, CloudStorage



bucket_name = 'rtf_staging'
PROJECT_ID='essence-analytics-dwh'
service_account_email='131786951123-compute@developer.gserviceaccount.com'

def moat_report_extract(tile_id):
    #build moat tile


    pass

#get and save report
# upload to gcs bucet
# uplaod to bq

## follow pattern of dfa dag