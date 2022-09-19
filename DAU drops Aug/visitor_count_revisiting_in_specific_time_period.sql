--DAU drop check for specific time period
--Lets say on 15th Sept 6:30-9:00 (AM) is issue time (visitor count A)
--Observed on 16th Sept 12:30 PM
--What % of A, comes back from 15th Sept 9:00 AM to 16th Sept 12:30 PM
 
SELECT COUNT (distinct visiting_back) from 
(
(SELECT distinct deviceID as visiting_back 
from maximal-furnace-783.sc_analytics.home_opened
WHERE time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-09-15 09:00:00.000000") AND time(datetime"2022-09-15 23:59:59.000000")
AND (date(time, "Asia/Kolkata") IN ("2022-09-15"))
AND deviceId IN
(SELECT distinct deviceId AS DAU 
from maximal-furnace-783.sc_analytics.home_opened
WHERE (time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-09-15 06:30:00.000000") AND time(datetime"2022-09-15 09:00:00.000000"))
AND date(time, "Asia/Kolkata")="2022-09-15"))

UNION ALL
(SELECT distinct deviceID as visiting_back 
from maximal-furnace-783.sc_analytics.home_opened
WHERE time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-09-16 00:00:00.000000") AND time(datetime"2022-09-16 12:30:59.000000")
AND (date(time, "Asia/Kolkata") IN ("2022-09-16"))
AND deviceId IN
(SELECT distinct deviceId AS DAU 
from maximal-furnace-783.sc_analytics.home_opened
WHERE (time(time, "Asia/Kolkata") BETWEEN time(datetime "2022-09-15 06:30:00.000000") AND time(datetime"2022-09-15 09:00:00.000000"))
AND date(time, "Asia/Kolkata")="2022-09-15"))
)
