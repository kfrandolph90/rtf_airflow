materialize_verification = """
                            SELECT
                              date,
                              CAST(REGEXP_EXTRACT(placement,r"OPID-(\d+)") AS int64) AS opid,
                              SUM(verifiable_impressions) verifiable_impressions_no_issues
                            FROM
                              `essence-analytics-dwh.rtf_brand_reporting.DCM_contentVerification`
                            WHERE
                              classifier = "None Detected"
                            GROUP BY
                              1,
                              2

                            """

materialize_player_report = """
                            WITH
                              player_pivot AS (
                              SELECT
                                date,
                                CAST(REGEXP_EXTRACT(placement,r"OPID-(\d+)") AS int64) AS opid,
                                CASE
                                  WHEN video_player_size = "HD" THEN verifiable_impressions
                              END
                                AS hd_player_impressions,
                                CASE
                                  WHEN video_player_size = "LARGE" THEN verifiable_impressions
                              END
                                AS large_player_impressions,
                                CASE
                                  WHEN video_player_size = "SMALL" THEN verifiable_impressions
                              END
                                AS small_player_impressions,
                                CASE
                                  WHEN video_player_size = "(not set)" THEN verifiable_impressions
                              END
                                AS player_not_set_impressions
                              FROM
                                `essence-analytics-dwh.rtf_brand_reporting.DCM_playerSize`)
                            SELECT
                              date,
                              opid,
                              SUM(hd_player_impressions) hd_player_impressions,
                              SUM(large_player_impressions) large_player_impressions,
                              SUM(small_player_impressions) small_player_impressions,
                              SUM(player_not_set_impressions) player_not_set_impressions
                            FROM
                              player_pivot
                            GROUP BY
                              1,
                              2
                            """


moat_vid = """
            SELECT
              date,
              CAST(REGEXP_EXTRACT(level4_label,r"OPID-(\d+)") AS int64) AS opid,
              SUM(impressions_analyzed) AS moat_vid_impressions_analyzed,
              SUM(susp_valid) AS moat_vid_susp_valid,
              SUM(valid_and_viewable) AS valid_and_viewable,
              SUM(reached_first_quart_sum) reached_first_quart_sum,
              SUM(reached_second_quart_sum) reached_second_quart_sum,
              SUM(reached_third_quart_sum) reached_third_quart_sum,
              SUM(reached_complete_sum) reached_complete_sum,
              SUM(player_visible_on_complete_sum) player_visible_on_complete_sum,
              SUM(player_audible_on_complete_sum) player_audible_on_complete_sum,
              SUM(player_vis_and_aud_on_complete_sum) player_vis_and_aud_on_complete_sum,
              SUM(susp_valid_and_inview_gm_meas_sum) susp_valid_and_inview_gm_meas_sum,
              sum(_5_sec_in_view_impressions) _5_sec_in_view_impressions,
              sum(susp_bot_geo_perc) susp_bot_geo_perc
            FROM
              `essence-analytics-dwh.rtf_brand_reporting.MOAT_videoTiles_RAW`

            Group by 1,2
            """

helper_sql.moat_disp = """
            SELECT
              date,
              CAST(REGEXP_EXTRACT(level3_label,r"OPID-(\d+)") AS int64) AS opid,
              SUM(impressions_analyzed) AS impressions_analyzed,
              SUM(valid_and_viewable) AS valid_and_viewable,
              SUM(iva) AS iva,
              SUM(susp_bot_geo_perc) AS susp_bot_geo_perc
            FROM
              `essence-analytics-dwh.rtf_brand_reporting.MOAT_displayTiles_RAW`
            GROUP BY
              1,
              2
            """

