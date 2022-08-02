select DATE(event_server_time_hour_level_ts) as event_server_time_date, 
sum(rawEventCount) as rawEventCountForDay, 
sum(errorCount) as errorCountForDay,
sum(rawEventCount) - sum(errorCount) as diff
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated` 
WHERE event_type_id = 62 AND date(event_server_time_hour_level_ts)>="2022-07-30"
GROUP BY 1
ORDER BY 1 DESC
