import json
import requests
from io import StringIO,FileIO
import logging
import time
from math import ceil



class MoatTile:
    
    base_metrics = ["impressions_analyzed",
                    "susp_human",
                    "human_and_viewable"] 

    vid_metrics = [ "player_vis_and_aud_on_complete_sum",                   
                    "player_audible_on_complete_sum",
                    "player_visible_on_complete_sum",
                    "reached_first_quart_sum",
                    "reached_second_quart_sum",
                    "reached_third_quart_sum",
                    "reached_complete_sum",
                    "avg_real_estate_perc",
                    "player_audible_full_vis_half_time_sum",
                    "5_sec_in_view_impressions",
                    "susp_human_and_inview_gm_meas_sum"]

    disp_metrics = ["iva",
                    "moat_score"]

    def __init__(self, tile_id, tile_type, name, level_filters=None, dimensions=None, **kwargs):
        self.brandid = tile_id
        self.tile_type = tile_type
        self.name = name
        self.filters = level_filters
        self.last_request_time = None
        self.time_since_last_request = None
        
        #self.campaigns = campaigns
        
        
        if not dimensions:
            self.dimensions = ["level1"]
        else:
            self.dimensions = dimensions
            
        if self.tile_type == "disp":
            self.metrics = MoatTile.base_metrics + MoatTile.disp_metrics
        elif self.tile_type == "video" or "vid":
            self.metrics = MoatTile.base_metrics + MoatTile.vid_metrics
    
    
    def request(self,query,token): 
        logging.info("API Request Time")
        if self.last_request_time:
            self.time_since_last_request = self.last_request_time - time.time()
        
        if self.time_since_last_request and self.time_since_last_request < 12:
            wait_time = 12 - ceil(self.time_since_last_request)
            logging.info("Sleep for rate limiting. Wait {}".format(wait_time))
            time.sleep(wait_time)
        
        auth_header = 'Bearer {}'.format(token)
        
        try:
            resp = requests.get('https://api.moat.com/1/stats.json',
                                params=query,
                                headers={'Authorization': auth_header,
                                            'User-agent': 'Essence Global 1.0'}
                               )
            self.last_request_time = time.time()
            if resp.status_code == 200:
                r = resp.json()
                return r
                #self.data.extend(details)
                #logging.info("Stored {} entries for {}".format(len(data),campaign))
            elif resp.status_code == 400:
                logging.error("Ya Goofed. Resp:{}".format(query))            
            
            
        except Exception as e:
            print("bad news")
            logging.error("Request Failure {}".format(e))
            return
        
        self.last_request_time = time.time()
        
        
    
    def clean_row(row):
        for k,v in row.items():
            if k == "5_sec_in_view_impressions":
                row["_5_sec_in_view_impressions"] = v
                del row["5_sec_in_view_impressions"]
            if k == "level3_id" and v == "ghostery":
                row[k] = ''
        return row
    
    def save_json_newline(self,data):
        if data != []:
            with open(self.filename, "w") as f:
                rows_cleaned = [clean_row(row) for row in data]       
                rows = [json.dumps(row) for row in rows_cleaned]
                row_str = '\n'.join(rows)
                f.write(row_str)
                logging.info("{} Saved".format(self.filename))
                return self.filename
        else:
            logging.info("No Data Bro!")
            return None
            
    def get_data(self, start_date, end_date,token,response=False):
        self.filename = str(self.brandid) + "_" + self.name + ".json"
        
        fields = self.dimensions + self.metrics
        
        self.query = {
                'metrics': ','.join(fields),
                'start': start_date,
                'end': end_date,
                'brandId':self.brandid, ## this is the tile ID 
                } 
        
        """
        if self.filters:
            for dimension,values in self.filters.items():
                if type(values) == list:
                    queries = [self.query.update({dimension:value}) for value in values]
                else:
                    self.query.update({dimension:values})
        """  
        
        
        if self.filters:
            self.query.update(self.filters)
        
        resp = self.request(self.query,token)
        
        if response == True:
            return resp
            
            
        
        
        if resp.get('data_available') == True:
            print("Data Available, Storing")
            self.save_json_newline(resp.get('results').get('details'))
            return self.filename
        else:
            logging.info('No Data for {} in that date range'.format(self.query.get('brandId')))
            return
        

        
        
        
        
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