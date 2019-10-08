import json
import os
import logging
from datetime import datetime,timedelta
from essence.analytics.platform import securedcredentials as secure_creds

try:
    from RTF.rtf_utils.moat_utils import MoatTile
    from RTF.rtf_utils.gcp_utils import BigQuery, CloudStorage
    from RTF.rtf_utils.bq_schema import moat_schema_dict
except:
    from rtf_utils.moat_utils import MoatTile
    from rtf_utils.gcp_utils import BigQuery, CloudStorage
    from rtf_utils.bq_schema import moat_schema_dict

bucket_name = 'rtf_staging'
PROJECT_ID='essence-analytics-dwh'
service_account_email='131786951123-compute@developer.gserviceaccount.com'

def moat_report_extract(tile_id,bucket_name,folder,**context):    
    yesterday = context['yesterday_ds_nodash'] ## yyyyddmm
    credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
    logging.info("Loaded Credentials")

    moat_token = secure_creds.getDataFromEssenceVault('Moat_Token_Google')
    logging.info("Loaded Token")

    filters = context.get('level_filters')
    dimensions = context.get('dimensions')

    tile = MoatTile(tile_id=tile_id,level_filters=filters,dimensions=dimensions)
    logging.info("Tile Instantiated")

    local_filename = tile.get_data(yesterday,
                                    yesterday,
                                    moat_token)

    logging.info("Data Stored {}".format(local_filename))
    
    gcs = CloudStorage(credentialsFromVault)
    
    logging.info("Upload to GCS")

    dest_blob_name = folder + "/" + local_filename

    blob = gcs.upload_blob(bucket_name = bucket_name,
                            destination_blob_name = dest_blob_name,
                            source=local_filename,
                            mode='filename')

    return blob

def load_bq(tile_id,file_uri,table,**context):
    yesterday = context['yesterday_ds_nodash']
    
    schema = moat_schema_dict.get(tile_id)
    if schema:
        logging.info("Schema Found")

    credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
    bq = BigQuery(credentialsFromVault)
    
    logging.info("Build BQ Job")
    resp = bq.load_from_gcs("rtf_brand_reporting",
                    file_uri,
                    "{}_{}".format(table,yesterday) ,
                    schema=schema,
                    extension='json')

    logging.info("START JOB: {}".format(resp.job_id))

    resp.result() ## in theory this waits for job to finish
    print("JOB COMPLETE: {}".format(resp.job_id))


def clean_up(bucket,folder,**context):       
    credentialsFromVault=secure_creds.getCredentialsFromEssenceVault(service_account_email)
    gcs = CloudStorage(credentialsFromVault)

    blobs = gcs.list_blobs(bucket_name=bucket,prefix=folder)

    for blob in blobs:
        blob.delete()
    
    logging.info("Blobs Cleaned")