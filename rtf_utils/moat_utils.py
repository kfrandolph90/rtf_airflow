import json
import requests
from io import StringIO,FileIO
import logging
from time import sleep

class MoatTile:

    base_request = ["date","level",
                    "impressions_analyzed",
                    "susp_human",
                    "human_and_viewable"
                    "susp_bot_geo_perc"] 

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

    disp_metrics = ["impressions_analyzed",
                    "susp_human",
                    "human_and_viewable",
                    "iva",
                    "moat_score",
                    "susp_bot_geo_perc"]

    def __init__(self, tile_id, name, campaigns, tile_type, social=None, **kwargs):
        self.brandid = tile_id
        self.name = name
        self.campaigns = campaigns
        self.tile_type = tile_type
        
        ## social usually has camapign at level2 (level1 is account)
        if social:
            self.campaign_level = "level2"
            self.base_request = MoatTile.base_request + ['level2']
        else:
            self.campaign_level = "level1"
            self.base_request = MoatTile.base_request + ['level1','level4']
            
        if self.tile_type == "disp":
            self.metrics = self.base_request + MoatTile.disp_metrics
        else:
            self.metrics = self.base_request + MoatTile.vid_metrics
    
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

