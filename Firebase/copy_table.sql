--Creating copy of 9th & 10th June data from session_time_hourly

CREATE OR REPLACE TABLE maximal-furnace-783.data_platform_temp1.sess_hrly_copy_9_10jun22
AS (SELECT * from maximal-furnace-783.feed_pod_data.session_time_hourly
WHERE DATE(event_start_time) IN ('2022-06-09', '2022-06-10'))
