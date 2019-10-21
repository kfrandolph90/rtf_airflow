# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#   Some helpful wrappers for gcp
#   RTF - Kyle.Randolph@essenceglobal.com
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
 

from google.cloud import storage,bigquery
import logging



"""
TODO
    - Cloud Storage Class

"""

class BigQuery:
    def __init__(self,creds):
        self.client = bigquery.client.Client(credentials=creds)
        
    def return_query(self,query):
        self.last_query = query
        results = self.client.query(query, location="US")
        return results
    
    def store_query_results(self,query,dataset_id,dest_table_id):
        """
        Use for materialized/static tables
        """
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        table_ref = self.client.dataset(dataset_id).table(dest_table_id)        
        job_config.destination = table_ref        
        query_job = self.client.query(query,location='US',job_config=job_config)
        return query_job.result() 
    
    def load_from_gcs(self,dataset_id,file_uri,dest_table,schema=None,mode=None,extension="csv",**kwargs):
        """
        Gets data for tile dimenions/filters within data range. 

        Cleans and saves file locally
        
        Args:
            dataset_id (int): request start date in YYYYMMDD
            file_uri (str): request end date in YYYYMMDD        
            
            dest_table (str): API token

            schema (list): list of BigQuery.SchemaObject. defaults to none which inferes schemea
            
            mode (str)(opt): defaults to WRITE_TRUNCATE disposition
            
            extension: (str): csv or json

        Returns:
            filename (str): filename of local file in working dir 
        """

        
        job_config = bigquery.LoadJobConfig()
        dataset_ref = self.client.dataset(dataset_id)
        
        if mode == "Append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND    
        elif mode == "Empty":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY        
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        if extension=="json":
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        else:
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.field_delimiter = ","
         
        ##Optional

        if schema:
            job_config.schema = schema
        else:
            job_config.autodetect = True
        
        
        if kwargs.get("skip_rows"):
            job_config.skip_leading_rows = kwargs.get("skip_rows")            
        
        load_job = self.client.load_table_from_uri(file_uri,
                                                    dataset_ref.table(dest_table),
                                                    location="US",  
                                                    job_config=job_config)
        
        #logging.info(load_job.job_id)
        return load_job

class CloudStorage:
    def __init__(self,creds):
        self.client = storage.client.Client(credentials=creds)
        
    def list_blobs(self,bucket_name,prefix=None):
        """Lists all the blobs in the bucket."""
        
        bucket = self.client.get_bucket(bucket_name)
        if prefix:
            blobs = bucket.list_blobs(prefix=prefix)
        else:
            blobs = bucket.list_blobs()
        return blobs

    def upload_blob(self,bucket_name, destination_blob_name, source, mode):
        """Uploads a file to the bucket."""
        if mode not in ["string","filename"]:
            raise SyntaxError    

        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        if mode == "string":
            blob.upload_from_string(source)
        elif mode == "filename":
            blob.upload_from_filename(source)

        logging.info('File uploaded as {}.'.format(destination_blob_name))

        return blob

    def copy_blob(self,bucket_name, blob_name, new_bucket_name, new_blob_name):
        """Copies a blob from one bucket to another with a new name."""
        source_bucket = self.client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = self.client.get_bucket(new_bucket_name)

        new_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name)

        logging.info('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
            source_blob.name, source_bucket.name, new_blob.name,
            destination_bucket.name))

    def delete_blob(self,bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        logging.info('Blob {} deleted.'.format(blob_name))

    def get_blob(self,bucket,blob):
        bucket = self.client.get_bucket(bucket)
        blob = bucket.get_blob(blob)
        return blob

    
    
        