--INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_ORGANIZATION
SELECT date(start_timestamp), EXTRACT(HOUR FROM start_timestamp) as hour, dataset_id, table_id, SUM(total_requests), SUM(total_rows)
FROM `region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_ORGANIZATION
WHERE
date(start_timestamp) BETWEEN "2022-08-22" AND "2022-08-29"
AND dataset_id LIKE "%moj_analytics%"
AND table_id LIKE "%video_play%"

GROUP BY 1,2,3, 4
ORDER BY 1 ASC, 2 ASC, 5 DESC, 6 DESC


-------
SELECT date(time, "Asia/Kolkata") as date, TIMESTAMP_TRUNC(time, HOUR, "Asia/Kolkata") as hr, 
COUNT(DISTINCT deviceId) as DAU,
SUM(duration) as duration FROM `maximal-furnace-783.moj_analytics.video_play_requested_event` 
WHERE (DATE(time, "Asia/Kolkata") IN ("2022-08-23", "2022-08-24", "2022-08-25","2022-08-26", "2022-08-29"))
AND (lower(tenant) like 'moj' or tenant is null)
and (lower(clientType) like 'android' or clientType is null )
GROUP BY 1,2 
ORDER BY 1, 2



--------
SELECT date(time, "Asia/Kolkata") as date, TIMESTAMP_TRUNC(time, HOUR, "Asia/Kolkata") as hr, 
COUNT(DISTINCT deviceId) as DAU,
SUM(duration) as duration FROM `maximal-furnace-783.moj_analytics.video_play_requested_event` 
WHERE (DATE(time, "Asia/Kolkata") IN ("2022-08-23", "2022-08-24", "2022-08-25","2022-08-26", "2022-08-29"))
AND (lower(tenant) like 'moj' or tenant is null)
and (lower(clientType) like 'android' or clientType is null )
GROUP BY 1,2 
ORDER BY 1, 2
