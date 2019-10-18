##### Moat Schemas for Brand Reporting #####

base_schema = [
{"name":"loads_unfiltered","type": "INTEGER","mode": "NULLABLE"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"human_and_viewable","type": "INTEGER","mode": "NULLABLE"},
{"name":"valid_and_viewable","type": "INTEGER","mode": "NULLABLE"}
]

video_schema = [   
{"name":"reached_first_quart_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "INTEGER","mode": "NULLABLE"},
]


moat_schemas = {

2506 :[
{"name":"date","type":"DATE","mode":"REQUIRED"},
{"name":"level1_id","type":"INTEGER","mode":"REQUIRED"},
{"name":"level1_label","type":"STRING", "mode":"REQUIRED"},
{"name":"level3_id","type":"INTEGER", "mode":"REQUIRED"},
{"name":"level3_label","type":"STRING", "mode":"REQUIRED"}] + base_schema + [
{"name":"human_and_viewable", "type":"INTEGER", "mode":"NULLABLE"},
{"name":"valid_and_viewable", "type":"INTEGER", "mode":"NULLABLE"},
{"name":"moat_score", "type":"INTEGER", "mode":"NULLABLE"},
{"name":"iva", "type":"INTEGER", "mode":"NULLABLE"}
],   

6179366 : [
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"}] + base_schema + [
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"}] + video_schema,


6178833 : [
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"}] + base_schema + [
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"}
] + video_schema,

13386: [
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"}] + base_schema + [
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"}
] + video_schema,

2698 :[
{"name":"date","type": "DATE","mode": "REQUIRED"}, # make required
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level3_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level3_label","type": "STRING","mode": "REQUIRED"}] + base_schema + 
[
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"}] + video_schema
    }

"""
## fix these later
13332 : [
{"name":"date","type": "DATE","mode": "REQUIRED"}, # make required
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"}, # make required
{"name":"level1_label","type": "STRING","mode": "REQUIRED"}, # make required
{"name":"level3_id","type": "INTEGER","mode": "REQUIRED"}, # make required
{"name":"level3_label","type": "STRING","mode": "REQUIRED"}, # make required
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]

13120 : {
'name' :"Essence Google Instagram","type":
'bq_name' : 'vid_instagram',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},

{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]},




6195505 : {
'name' : "Essence Google Facebook NA","type":
'bq_name' : 'vid_facebook_main_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED")]},


6195510 : {
'name' :"Essence Google Instagram NA","type":
'bq_name' : 'vid_instagram_main_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_first_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]},


6195511 : {
'name' : "Essence Google Instagram Stories NA" ,
'bq_name' : 'vid_instagram_stories_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_first_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]
            },

6142389 : {
'name' : "Essence Google Snapchat","type":
'bq_name' : 'vid_snapchat_main',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level3_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level3_label","type": "STRING","mode": "REQUIRED"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE")]
},


6146664 : {
'name' : "Google Hulu","type":
'bq_name' : 'vid_hulu_main',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_first_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]
},







6180313 : {
'name': "Google Hulu In App","type":
'bq_name' : 'vid_hulu_appp',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"_5_sec_in_view_impressions","type": "INTEGER","mode": "NULLABLE"},
{"name":"avg_real_estate_perc","type": "FLOAT","mode": "NULLABLE"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_first_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]
    },


6195543 : {
'name':"Essence Google Twitter NA","type":
'bq_name' : 'vid_twitter_main_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE"},
{"name":"player_audible_full_vis_half_time_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_audible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_vis_and_aud_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"player_visible_on_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_complete_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_first_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_second_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"reached_third_quart_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_human_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid","type": "FLOAT","mode": "NULLABLE"},
{"name":"susp_valid_and_inview_gm_meas_sum","type": "FLOAT","mode": "NULLABLE")]},








6195541 : {
'name' : "Google Twitter Display NA","type":
'bq_name' : 'disp_twitter_main_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE")]},


6188035 : {
'name' : "Essence Google Instagram Display","type":
'bq_name' : 'disp_instagram_main',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE")]},


6195427 : {
'name' : "Essence Google Instagram Stories Display NA","type":
'bq_name' : 'disp_instagram_stories_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level2_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level2_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE")
]},


6195503 : {
'name' : "Essence Digital Facebook Display Extended Metrics NA","type":
'bq_name' : 'disp_facebook_extended_na',
'schema':[
{"name":"date","type": "DATE","mode": "REQUIRED"},
{"name":"level1_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level1_label","type": "STRING","mode": "REQUIRED"},
{"name":"level4_id","type": "INTEGER","mode": "REQUIRED"},
{"name":"level4_label","type": "STRING","mode": "REQUIRED"},
{"name":"impressions_analyzed","type": "INTEGER","mode": "NULLABLE")
]}

}
"""