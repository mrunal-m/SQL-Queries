--DAU with time
SELECT TIMESTAMP_TRUNC(time, MINUTE, "Asia/Kolkata") as minute, 
count(distinct deviceId) AS DAU 
from maximal-furnace-783.moj_analytics.home_opened
WHERE (DATE(time) BETWEEN "2022-08-21" AND "2022-08-25")
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null )
GROUP BY 1
ORDER BY 1 ASC

--DAU with servertime
SELECT TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(servertime), MINUTE, "Asia/Kolkata") as minute, 
count(distinct deviceId) AS DAU 
from maximal-furnace-783.moj_analytics.home_opened
WHERE (DATE(time) BETWEEN "2022-08-21" AND "2022-08-25")
AND (DATE(TIMESTAMP_MILLIS(servertime), "Asia/Kolkata") BETWEEN "2022-08-23" AND "2022-08-25") 
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null )
GROUP BY 1
ORDER BY 1 ASC

-- same with ntp_EventRecordTime/DistpactTime
