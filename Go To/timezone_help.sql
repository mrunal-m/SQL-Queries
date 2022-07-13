--Comparing hourly numbers for intraday & full day
--set @@time_zone = "Asia/Kolkata"

SELECT TIMESTAMP_TRUNC(TIMESTAMP(DATETIME(event_start_time, "Asia/Kolkata")), HOUR) as hour, 
COUNT (*) as rowCount
from maximal-furnace-783.data_platform_temp1.test1_session_time_hourly
WHERE date(event_start_time, "Asia/Kolkata") = "2022-07-09"
GROUP BY 1
ORDER BY 1


