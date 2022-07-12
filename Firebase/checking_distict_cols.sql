SELECT COUNT(DISTINCT(event_start_time)) as event_start_time, 
COUNT(DISTINCT(user_id)) as user_id, COUNT(DISTINCT(ga_session_id)) as ga_session_id, 
COUNT(DISTINCT(ga_session_number)) as ga_session_number, 
COUNT(DISTINCT(event_start_timestamp)) as event_start_timestamp 
from maximal-furnace-783.feed_pod_data.session_time_hourly
WHERE date(event_start_time) ="2022-07-08"


SELECT COUNT(DISTINCT(event_start_time)) as event_start_time, 
COUNT(DISTINCT(user_id)) as user_id, COUNT(DISTINCT(ga_session_id)) as ga_session_id, 
COUNT(DISTINCT(ga_session_number)) as ga_session_number, 
COUNT(DISTINCT(event_start_timestamp)) as event_start_timestamp 
from maximal-furnace-783.data_platform_temp1.test1_session_time_hourly
WHERE date(event_start_time) ="2022-07-08"
