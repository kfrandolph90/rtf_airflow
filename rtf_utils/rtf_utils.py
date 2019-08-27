from google.cloud import bigquery, storage
import requests
import os
import json
import time
from time import sleep
import logging
from io import StringIO,FileIO

from googleapiclient import discovery, http
from oauth2client import client

import random


############ GCS Utils ############

def list_blobs(bucket_name,prefix=None):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    if prefix:
        blobs = bucket.list_blobs(prefix=prefix)
    else:
        blobs = bucket.list_blobs()
    return blobs

def upload_blob(bucket_name, destination_blob_name, source, mode):
    """Uploads a file to the bucket."""
    if mode not in ["string","filename"]:
        raise SyntaxError    

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    if mode == "string":
        blob.upload_from_string(source)
    elif mode == "filename":
        blob.upload_from_filename(source)
    
    print('File uploaded as {}.'.format(destination_blob_name))

def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """Copies a blob from one bucket to another with a new name."""
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)

    print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print('Blob {} deleted.'.format(blob_name))

def get_blob(bucket,blob):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.get_blob(blob)
    return blob


############ BQ Utils ############

def bq_query(query):
    client = bigquery.Client()
    '''
    query = (
        "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` "
        'WHERE state = "TX" '
        "LIMIT 100"
    )
    '''
    
    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
    )  # API request - starts the query

    for row in query_job:  # API request - fetches results
        # Row values can be accessed by field name or index
        assert row[0] == row.name == row["name"]
        print(row)

def bq_load(dataset_id,file_uri,bq_schema,dest_table,filetype,mode=None):
    ## switch to logging
    ## uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"
    
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()

    if mode == "Append":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    if mode == "Overwrite":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY


    job_config.schema = bq_schema
    
    if filetype == "json":
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    
    elif filetype == "csv":
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1

    else:
        raise NotImplementedError
    
    load_job = client.load_table_from_uri(
        file_uri,
        dataset_ref.table("dest_table"),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("us_states"))
    print("Loaded {} rows.".format(destination_table.num_rows))

############ Platform Utils ############
class MoatTile:
    with open("/home/airflow/gcs/dags/RTF/moat_config_pixel.json") as json_file:
        config = json.load(json_file)

    def __init__(self, tile_id, name, campaigns, tile_type, social=None, **kwargs):
        self.brandid = tile_id
        self.name = name
        self.campaigns = campaigns
        self.tile_type = tile_type
        
        if social:
            self.campaign_level = "level2"
            self.base_metrics = ['date','level2','level3']
        else:
            self.campaign_level = "level1"
            self.base_metrics = ['date','level1','level3','level4']
            
        if self.tile_type == "disp":
            self.metrics = self.base_metrics + MoatTile.config['metrics']['disp_metrics']
        else:
            self.metrics = self.base_metrics + MoatTile.config['metrics']['vid_metrics']
    
    def get_data(self, start_date, end_date,token):
        auth_header = 'Bearer {}'.format(token)        
        query = {
                'metrics': ','.join(self.metrics),
                'start': start_date,
                'end': end_date,
                'brandId':self.brandid, ## this is the tile ID 
                }         
        
        self.data = []
        
        for campaign in self.campaigns:
            query[self.campaign_level] = campaign
            logging.info("Getting Data for {}\n {}-{}".format(campaign,start_date,end_date))
            
            try:
                resp = requests.get('https://api.moat.com/1/stats.json',
                                    params=query,
                                    headers={'Authorization': auth_header,
                                                'User-agent': 'Essence Global 1.0'}
                                   )
            
                if resp.status_code == 200:
                    r = resp.json()
                    details = r.get('results').get('details')
                    self.data.extend(details)
                    logging.info("Stored {} entries for {}".format(len(details),campaign))
                elif resp.status_code == 400:
                    logging.error("Ya Goofed. Query:\n{}".format(query))            
                   
            except Exception as e:
                logging.error("Request Failure {}".format(e))
                pass
            
            sleep(11)

def clean_row(row):
    for k,v in row.items():
        if k == "5_sec_in_view_impressions":
            row["_5_sec_in_view_impressions"] = v
            del row["5_sec_in_view_impressions"]
        if k == "level3_id" and v == "ghostery":
            row[k] = ''
    return row

def format_json_newline(data):
    buf = StringIO()
    rows_cleaned = [clean_row(row) for row in data]
    rows = [json.dumps(row) for row in rows_cleaned]
    row_str = '\n'.join(rows)    
    buf.write(row_str)
    buf.seek(0)
    return buf.getvalue()



def cleanup(filename):
    try:
        os.remove(filename)
    except OSError as e: 
        print ("Error: %s - %s." % (e.filename, e.strerror))


def clean_dcm_file(csvobj):
    data = []
    write = False
    reader = csv.reader(csvobj, delimiter=',')
    for row in reader:
        if write == True:
            data.append(row)        
        elif row == ['Report Fields']:
            write = True

    if data[-1][0] == 'Grand Total:':
        data.pop()
    return data

output = io.StringIO()
writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
for row in a:
    writer.writerow(row)

############ DCM/DFA Utils ############
CHUNK_SIZE = 32 * 1024 * 1024
MIN_RETRY_INTERVAL = 10
# Maximum amount of time between polling requests. Defaults to 10 minutes.
MAX_RETRY_INTERVAL = 10 * 60
# Maximum amount of time to spend polling. Defaults to 1 hour.
MAX_RETRY_ELAPSED_TIME = 60 * 60

def run_report(service, profile_id, report_id):
    report_file = service.reports().run(
      profileId=profile_id, reportId=report_id).execute()
    return report_file
    

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 3 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))


def wait_for_report_file(service,report_id, file_id):    
    sleep = 0
    tries = 0
    start_time = time.time()
    
    while True:
        report_file = service.files().get(reportId=report_id, fileId=file_id).execute()
        status = report_file['status']
        
        if status == 'REPORT_AVAILABLE':
            logging.info("Report All Good")
            return
        elif status != 'PROCESSING':
            logging.info("Error: {}".format(status))
            return
        
        elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
            logging.info('File processing deadline exceeded.')
            return

        tries+=1
        logging.info("Report not ready, tries #{}".format(tries))
        sleep = next_sleep_interval(sleep)
        

def generate_file_name(report_file):
    """Generates a report file name based on the file metadata."""
  # If no filename is specified, use the file ID instead.
    file_name = report_file['fileName'] or report_file['id']
    extension = '.csv' if report_file['format'] == 'CSV' else '.xml'
    return file_name + extension
        
def download_file(service,report_id,file_id):
    try:
        report_file = service.files().get(reportId=report_id,fileId=file_id).execute()
        file_name = generate_file_name(report_file)
        if report_file['status'] == 'REPORT_AVAILABLE':
            out_file = FileIO(file_name, mode='wb')

            request = service.files().get_media(reportId=report_id, fileId=file_id)

            downloader = http.MediaIoBaseDownload(out_file, request,
                                                chunksize=CHUNK_SIZE)

            download_finished = False

            while download_finished is False:
                _, download_finished = downloader.next_chunk()
    except client.AccessTokenRefreshError:
        print ('The credentials have been revoked or expired, please re-run the '
           'application to re-authorize')
    return file_name

