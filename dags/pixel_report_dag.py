from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from rtf_utils import bq_load_json,build_bq_schema
from datetime import datetime,timedelta

from RTF.rtf_utils.rtf_utils import MoatTile,upload_blob,format_json_newline

import logging

## Load Stuff
TOKEN = Variable.get('rtf_moat_token')

logging.INFO("Loaded MOAT Token")

dest_bucket = "rtf_staging"


tiles = [(2506,"google_display",[22443077],"disp",False),
            (2698,"google_video",[22443077],"vid",False),
            (6195541,"tw_display",[21017248,20969946,20970333,20969992],"disp",True),
            (6195543,"tw_display",[20970543],"vid",True),
            (8268,"fb_video",[23843331336980586],"vid",True),
            (6188035,"ig_disp",[23843331350810586],"vid",True),
            (6195503,"fb_video",[23843331338570586,23843331340260586],"vid",True),
            (13386,"yt_trueview",[7020493427],"vid",False),
            (6179366,"yt_reserve", [2520197687,2549731844,2550298543,2547410762],"vid",True)]


default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 5, 16), 
    'email_on_failure': False,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 1, 
    'retry_delay': timedelta(minutes=1),
    'provide_context':True # this makes xcom work with
}

dag = DAG('pixel_report_dag',
            default_args=default_args,
            description='rev. 1 dag for pixel_brand_report',
            schedule_interval=None, 
            start_date=datetime(2018, 3, 20), 
            catchup=False)


# get moat >> save json gcs >> json_bq >> materialize table

def moat_request_task(**context):
    # instantiates moattile from context args
    tile_id, tile_name, campaign_ids, tile_type, social = context["tile"] 
    tile = MoatTile(tile_id,tile_name,campaign_ids,tile_type,social)    
    start_date, end_date = context["date_range"]
    
    logging.info("Get Tile Data for {} - {} ".format(start_date,end_date))    
    tile.get_data(start_date,end_date,TOKEN) ##rewire to pull last 3 days
    filename = tile.name + start_date + '_' + end_date + ".json"
    logging.info("Moat Req Success: {} Rows".format(len(tile.data)))
    upload_str = format_json_newline(tile.data)    
    
    upload_blob(
        bucket_name = dest_bucket , 
        destination_blob_name = filename, 
        source = upload_str, 
        mode = "string"    
    )

    logging.info("Moat Req Success: {} Rows".format(len(tile.data)))

    return filename

    
    
    ## move to MoatTile as method eventually
   

start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id= "End", retries=0, dag=dag)

for tile in tiles:    
    task_id = tile[1] + "_" + str(tile[0])     
    moat_account_task = PythonOperator(task_id = task_id + "_moat_req", 
                                        python_callable = moat_request_task,
                                        op_kwargs = {
                                            "tile":tile,
                                            "date_range":("20190801","20190810")                                                
                                        },
                                        dag = dag)
    start_task >> moat_account_task >> end_task
