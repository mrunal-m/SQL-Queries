--Comparing hourly numbers for intraday & full day
SELECT TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64) /1000000 as INT64)), HOUR) as hour, 
COUNT (*) as rowCount
from sc-bigquery-product-analyst.firebase_intraday_testing.events_intraday_20220708
GROUP BY 1
ORDER BY 1
