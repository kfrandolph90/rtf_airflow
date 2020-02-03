SELECT
coalesce(a.week,b.week) as  week,
coalesce(a.Campaign,b.Campaign) as campaign,
coalesce(a.creative_format,b.creative_format) as creative_format,
coalesce(a.goal_category,b.goal_category) goal_category,
coalesce(a.frequency_range,b.frequency_range) frequency_range,
coalesce(a.unique_user_count+b.unique_device_count, a.unique_user_count,b.unique_device_count) reach,
coalesce(a.impressions+b.impressions,a.impressions,b.impressions) impressions,
'{{ ds }}' as date_updated
  
FROM
    `{{ params.adh_project_id }}.{{ params.adh_dataset }}.{{ params.main_table }}` a
full outer JOIN
    `{{ params.adh_project_id }}.{{ params.adh_dataset }}.{{ params.join_table }}` b
  
USING
  (Campaign,
    creative_format,
    goal_category,
    week,
    frequency_range)
    
ORDER BY 2,1,3,4
