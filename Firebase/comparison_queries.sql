--Comparing total entries, unique user ids & device ids
SELECT COUNT(*) as total_entries, COUNT(DISTINCT(user_id)) as user_id, 
COUNT(DISTINCT(deviceid)) as deviceid from maximal-furnace-783.feed_pod_data.session_time_hourly
WHERE date(event_start_time) IN ('2022-06-09', '2022-06-10', '2022-06-11')


SELECT COUNT(*) as total_entries, COUNT(DISTINCT(user_id)) as user_id, 
COUNT(DISTINCT(deviceid)) as deviceid from maximal-furnace-783.data_platform_temp1.sess_hrly_copy_9_10jun22
WHERE date(event_start_time) IN ('2022-06-09', '2022-06-10', '2022-06-11')


--Comparing timespent
SELECT date(event_start_time), app_id, (SUM(time_spent_millis)/(1000*60*60)) as timespent_hr
from maximal-furnace-783.feed_pod_data.session_time_hourly
WHERE date(event_start_time) IN ('2022-06-09', '2022-06-10', '2022-06-11')
GROUP BY 1, 2

SELECT date(event_start_time), app_id, (SUM(time_spent_millis)/(1000*60*60)) as timespent_hr
from maximal-furnace-783.data_platform_temp1.sess_hrly_copy_9_10jun22
WHERE date(event_start_time) IN ('2022-06-09', '2022-06-10', '2022-06-11')
GROUP BY 1, 2
