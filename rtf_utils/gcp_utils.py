# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#   Some helpful wrappers for dfa reporting.
#   RTF - Kyle.Randolph@essenceglobal.com
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

from google.cloud import storage,bigquery


"""
TODO
    - Cloud Storage Class

"""

class BigQuery:
    def __init__(self,creds):
        self.client = bigquery.Client(creds=creds)
        
    def return_query(self,query):
        self.last_query = query
        results = self.client.query(query, location="US")
        return results
    
    def store_query_results(self,query,dataset_id,dest_table_id):
        """
        Use for materialized/static tables
        """
        job_config = bigquery.QueryJobConfig()
        table_ref = self.client.dataset(dataset_id).table(dest_table_id)        
        job_config.destination = table_ref        
        query_job = self.client.query(query,location='US',job_config=job_config)
        return query_job.result() 
    
    def load_from_gcs(self,dataset_id,file_uri,schema,dest_table,mode,**kwargs):
        """
        Schema expects list of dictionaries contain columns with 'name' and 'type' keyss
        """
        job_config = bigquery.LoadJobConfig()
        dataset_ref = self.client.dataset(dataset_id)
        
        if mode == "Append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND    
        elif mode == "Empty":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY        
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            
        bq_schema = []
    
        for col in schema:
            bq_schema.append(bigquery.SchemaField(col.get('name'),col.get('type')))
            
        job_config.schema = bq_schema
        
        ## Implement extension check here
        
        job_config.source_format = bigquery.SourceFormat.CSV
        
        if kwargs.get("skip_rows"):
            job_config.skip_leading_rows = kwargs.get("skip_rows")            
        
        load_job = self.client.load_table_from_uri(file_uri,
                                                    dataset_ref.table(dest_table),
                                                    location="US",  
                                                    job_config=job_config)
        
        return load_job.result()

class CloudStorage:
    pass