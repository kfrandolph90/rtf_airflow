import json
import requests
from urllib3.exceptions import HTTPError
from io import StringIO,FileIO
import logging
import time
from math import ceil

class MoatTile:
    
    base_metrics = ["impressions_analyzed",
                    "loads_unfiltered",
                    "susp_human",
                    "human_and_viewable",
                    ] 

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

    """
    TODO: Update BQ friendly names in tiles_meta
    
    """
    tiles_meta = {
        13332:{'type':'video','name':'V_dcm-master'},
        2698:{'type':'video','name':'V_google_na'},
        6195505:{'type':'video','name':'V_facebook_na'},
        13120:{'type':'video','name':'V_instagram'},
        6195511:{'type':'video','name':'V_instagram-stories_na'},
        6142389:{'type':'video','name':'V_snapchat'},
        6146664:{'type':'video','name':'V_hulu'},
        6178833:{'type':'video','name':'V_youtube-reserve-programmatic'},
        6179366:{'type':'video','name':'V_youtube-reserve'},
        6180313:{'type':'video','name':'V_hulu-in-app'},
        6195543:{'type':'video','name':'V_twitter_na'},
        13386:{'type':'video','name':'V_youtube-trueview_na'},
        2506:{'type':'disp','name':'D_google_na'},
        6195541:{'type':'disp','name':'D_twitter_na'},
        6188035:{'type':'disp','name':'D_instagram'},
        6195427:{'type':'disp','name':'D_instagram-stories_na'},
        6195503:{'type':'disp','name':'D_facebook-ext-metrics_na'},
        6196284:{'type':'video','name':'V_youtube_progres'},
}

    def __init__(self, tile_id, level_filter=None, dimensions=None, **kwargs):
        self.brandid = tile_id
        self.tile_type = MoatTile.tiles_meta[tile_id]['type']
        self.name = MoatTile.tiles_meta[tile_id]['name']
        self.filters = level_filter
        self.last_request_time = None
        self.time_since_last_request = None

        if not dimensions:
            self.dimensions = ["level1","level2","level3","level4"]
        else:
            self.dimensions = dimensions
            
        if self.tile_type == "disp":
            self.metrics = MoatTile.base_metrics + MoatTile.disp_metrics
        elif self.tile_type == "video" or "vid":
            self.metrics = MoatTile.base_metrics + MoatTile.vid_metrics
    
    def __str__(self):
        return "Moat Tile {} - {} ".format(self.brandid,self.name)
    
    def request(self,query,token): 
        logging.info("API Request Time")
        if self.last_request_time:
            self.time_since_last_request = self.last_request_time - time.time()
        
        if self.time_since_last_request and self.time_since_last_request < 12:
            wait_time = 12 - ceil(self.time_since_last_request)
            logging.info("Sleep for rate limiting. Wait {}".format(wait_time))
            time.sleep(wait_time)
        
        auth_header = 'Bearer {}'.format(token)
        
        
        resp = requests.get('https://api.moat.com/1/stats.json',
                                params=query,
                                headers={'Authorization': auth_header,
                                            'User-agent': 'Essence Global 1.0'}
                               )
        self.last_request_time = time.time()

        try:
            resp.raise_for_status()
            if resp.status_code == 200:
                r = resp.json()
                return r 
            else:
                logging.error("Succesful, non-200 resp".format(resp.status_code))
                raise Exception## will raise exception for 4xx/3xx error codes   
            
        except Exception as e:
            logging.error(e)
            logging.error(resp.headers)
            raise Exception
        
    def clean_row(self,row):
        for k,v in row.items():
            if k == "5_sec_in_view_impressions":
                row["_5_sec_in_view_impressions"] = v
                del row["5_sec_in_view_impressions"]
            if k == "level3_id" and v == "ghostery":
                row[k] = ''
        return row
    
    def save_json_newline(self,data):
        if data != []: ## this might be failing
            with open(self.filename, "w") as f:
                rows_cleaned = [self.clean_row(row) for row in data]       
                rows = [json.dumps(row) for row in rows_cleaned]
                row_str = '\n'.join(rows)
                f.write(row_str)
                logging.info("{} Saved".format(self.filename))
                return self.filename
        else:
            logging.info("Empty Data")
            return None
            
    def get_data(self, start_date, end_date,token,response=False):
        """
        Gets data for tile dimenions/filters within data range. 

        Cleans and saves file locally
        
        Args:
            start_date (str): request start date in YYYYMMDD
            end_date (str): request end date in YYYYMMDD        
            token (str): API token

        Returns:
            filename (str): filename of local file in working dir
            or
            None : if request fails or data set is empty
        """
        
        if self.filters:
            filter_value = [*self.filters.values()][0]
            self.filename = "{}_{}.json".format(self.brandid,filter_value)
        else:
            self.filename = "{}.json".format(self.brandid)
        
        fields = self.dimensions + self.metrics
        
        self.query = {
                'metrics': ','.join(fields),
                'start': start_date,
                'end': end_date,
                'brandId':self.brandid, ## this is the tile ID 
                } 
        
        """
        TODO: Implement multiple filters behavior
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
        
        logging.debug(resp)
        
        if resp and resp.get('data_available') == True:
            return self.save_json_newline(resp.get('results').get('details')) #should return none if data empty

        else:
            logging.info('No Data for {} in that date range'.format(self.query.get('brandId')))
            return None
        

        
        
        