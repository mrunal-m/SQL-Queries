SELECT TIMESTAMP_TRUNC(time, MINUTE) as time,
SUM(case when ntp_eventRecordTime IS NULL THEN 1 ELSE 0 END), COUNT(*)
from maximal-furnace-783.moj_analytics.home_opened
WHERE date(time) BETWEEN "2022-08-22" AND "2022-08-23"
GROUP BY 1
ORDER BY 1 ASC
