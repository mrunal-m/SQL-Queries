with t1 as (SELECT event_name, (select evt_prms.value.int_value from UNNEST(event_params) as evt_prms 
where evt_prms.key = 'engagement_time_msec') as time_spent_millis
from  sharechat-firebase.analytics_163194662.events_20220803)

SELECT t1.event_name, COUNT(*) as total_rows, SUM(t1.time_spent_millis) from t1
GROUP BY 1
ORDER BY 2 DESC
