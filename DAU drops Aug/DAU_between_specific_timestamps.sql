--DAU drop check
-- memtioning specific time period
SELECT distinct deviceId AS DAU 
from maximal-furnace-783.moj_analytics.home_opened
WHERE time(time) BETWEEN time(datetime "2022-08-27 05:06:00.000000") AND time(datetime"2022-08-27 05:56:00.000000")
AND date(time)="2022-08-27"
AND (lower(tenant) like 'moj' or tenant is null) AND (lower(clientType) like 'android' or clientType is null )
