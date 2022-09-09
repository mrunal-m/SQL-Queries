--DAU drop check for specific time period
--Lets say on 26th Aug 20:45-21:45 (PM) is issue time (visitor count A)
--Observed on 27th Aug 10:00 AM
--What % of A, comes back from 26 Aug 21:45 to 27 Aug 10:00 
SELECT COUNT (distinct visiting_back) from (SELECT distinct deviceID as visiting_back 
from maximal-furnace-783.moj_analytics.home_opened
WHERE time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-08-23 14:20:00.000000") AND time(datetime"2022-08-23 23:59:59.000000")
AND (date(time, "Asia/Kolkata") IN ("2022-08-23"))
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null )
AND deviceId IN
(SELECT distinct deviceId AS DAU 
from maximal-furnace-783.moj_analytics.home_opened
WHERE (time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-08-23 13:26:00.000000") AND time(datetime"2022-08-23 14:20:00.000000"))
AND date(time, "Asia/Kolkata")="2022-08-23"
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null ))

UNION ALL
(SELECT distinct deviceID as visiting_back 
from maximal-furnace-783.moj_analytics.home_opened
WHERE time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-08-27 00:00:00.000000") AND time(datetime"2022-08-27 23:59:59.000000")
AND (date(time, "Asia/Kolkata") IN ("2022-08-24"))
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null )
AND deviceId IN
(SELECT distinct deviceId AS DAU 
from maximal-furnace-783.moj_analytics.home_opened
WHERE (time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-08-23 13:26:00.000000") AND time(datetime"2022-08-23 14:20:00.000000"))
AND date(time, "Asia/Kolkata")="2022-08-23"
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null ))
))
