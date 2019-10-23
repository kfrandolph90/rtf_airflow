from google.cloud import bigquery


base_schema = [
bigquery.SchemaField("loads_unfiltered", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
]

video_schema = [   
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
]



moat_schema_dict = {

## bq name

13332 : {
'name' : 'Master Video DCM',
'bq_name':'vid_dcm_master',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"), # make required
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"), # make required
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"), # make required
bigquery.SchemaField("level3_id", "INTEGER", mode = "REQUIRED"), # make required
bigquery.SchemaField("level3_label", "STRING", mode = "REQUIRED"), # make required
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),



bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")],
},

13120 : {
'name' :"Essence Google Instagram",
'bq_name' : 'vid_instagram',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),

bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},

2698 : {
'name' : 'Google NA 2015 to Present - Vid',
'bq_name' : 'vid_google_2015td_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"), # make required
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level3_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level3_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),

bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},


6195505 : {
'name' : "Essence Google Facebook NA",
'bq_name' : 'vid_facebook_main_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED")]},


6195510 : {
'name' :"Essence Google Instagram NA",
'bq_name' : 'vid_instagram_main_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},


6195511 : {
'name' : "Essence Google Instagram Stories NA" ,
'bq_name' : 'vid_instagram_stories_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]
            },

6142389 : {
'name' : "Essence Google Snapchat",
'bq_name' : 'vid_snapchat_main',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level3_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level3_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE")]
},


6146664 : {
'name' : "Google Hulu",
'bq_name' : 'vid_hulu_main',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]
},



6178833 : {
'name':"Essence Google Reserve Programmatic",
'bq_name' : 'vid_youtube_reserveprog',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]
            },


6179366 : {
'name':"Essence Google Reserve",
'bq_name' : 'vid_youtube_reserve',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},


6180313 : {
'name': "Google Hulu In App",
'bq_name' : 'vid_hulu_appp',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("avg_real_estate_perc", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]
    },


6195543 : {
'name':"Essence Google Twitter NA",
'bq_name' : 'vid_twitter_main_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},


13386: {
'name': "Google TrueView NA",
'bq_name' : 'vid_youtube_trueview_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("_5_sec_in_view_impressions", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_full_vis_half_time_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_audible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_vis_and_aud_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("player_visible_on_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_complete_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_first_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_second_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("reached_third_quart_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_human_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid", "FLOAT", mode = "NULLABLE"),
bigquery.SchemaField("susp_valid_and_inview_gm_meas_sum", "FLOAT", mode = "NULLABLE")]},


2506 : {
'name' : "Google NA 2015 to Present",
'bq_name' : 'disp_google_main_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level3_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level3_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE"),
bigquery.SchemaField("moat_score", "INTEGER", mode = "NULLABLE")]},


6195541 : {
'name' : "Google Twitter Display NA",
'bq_name' : 'disp_twitter_main_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE")]},


6188035 : {
'name' : "Essence Google Instagram Display",
'bq_name' : 'disp_instagram_main',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE")]},


6195427 : {
'name' : "Essence Google Instagram Stories Display NA",
'bq_name' : 'disp_instagram_stories_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level2_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level2_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE")
]},


6195503 : {
'name' : "Essence Digital Facebook Display Extended Metrics NA",
'bq_name' : 'disp_facebook_extended_na',
'schema':[
bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
bigquery.SchemaField("level1_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level1_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("level4_id", "INTEGER", mode = "REQUIRED"),
bigquery.SchemaField("level4_label", "STRING", mode = "REQUIRED"),
bigquery.SchemaField("impressions_analyzed", "INTEGER", mode = "NULLABLE")
]}

}
