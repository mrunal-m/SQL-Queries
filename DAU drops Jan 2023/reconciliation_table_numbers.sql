-- Reconciliation Pipeline
(
SELECT 27 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-27" 
AND (time(event_server_time_hour_level_ts) >= time(datetime "2023-01-27 17:30:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 28 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-28" 
AND (time(event_server_time_hour_level_ts) <= time(datetime "2023-01-28 07:10:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 20 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-20" 
AND (time(event_server_time_hour_level_ts) >= time(datetime "2023-01-20 17:30:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 21 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-21" 
AND (time(event_server_time_hour_level_ts) <= time(datetime "2023-01-21 07:10:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 13 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-13" 
AND (time(event_server_time_hour_level_ts) >= time(datetime "2023-01-13 17:30:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 14 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-14" 
AND (time(event_server_time_hour_level_ts) <= time(datetime "2023-01-14 07:10:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 6 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-06" 
AND (time(event_server_time_hour_level_ts) >= time(datetime "2023-01-06 17:30:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
UNION ALL
(
SELECT 7 as dt, event_type_id, SUM(rawEventCount) AS rawEventCount
from `sharechat-production.platform_events_reconciliation.event_ingestion_funnel_error_event_aggregated`
WHERE date(event_server_time_hour_level_ts) = "2023-01-07" 
AND (time(event_server_time_hour_level_ts) <= time(datetime "2023-01-07 07:10:00.000000"))
AND event_type_id IN (2, 79, 3, 6)
GROUP BY 1, 2
)
ORDER BY 1 ASC, 2 ASC
