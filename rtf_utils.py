from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery, storage

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    return blobs

def upload_blob(project_id, bucket_name, source, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(source)
    print('File uploaded to {}.'.format(destination_blob_name))
    return

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print('Blob {} deleted.'.format(blob_name))

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

def build_bq_schema(fields):
    return [bigquery.SchemaField(field[0],field[1]) for field in fields]
    

def bq_load_json(dataset_id,file_uri,bq_schema,dest_table,filetype,mode=None):
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
