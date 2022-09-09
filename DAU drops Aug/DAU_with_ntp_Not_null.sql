SELECT TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(SAFE_CAST(ntp_eventRecordTime AS INT64)), MINUTE, "Asia/Kolkata") as minute, 
count(distinct deviceId) as DAU
FROM `maximal-furnace-783.moj_analytics.home_opened` 
WHERE (DATE(time) BETWEEN "2022-08-23" AND "2022-08-24") AND 
(date(TIMESTAMP_MILLIS(SAFE_CAST(ntp_eventRecordTime AS INT64))) BETWEEN "2022-08-23" AND "2022-08-24")
and (lower(tenant) like 'moj' or tenant is null)
and (lower(clientType) like 'android' or clientType is null )
and ntp_eventRecordTime IS NOT NULL
GROUP BY 1
ORDER BY 1 ASC
