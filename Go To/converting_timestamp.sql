select --date(timestamp_seconds(safe_cast(substr(createdOn, 1, 10) as int64))) as dt,
round((split(latLong, ','))[safe_ordinal(1)], 6) as latitude, (split(latLong, ','))[safe_ordinal(2)] as longitude,
COUNT(*)
from `maximal-furnace-783.sc_analytics.user`
WHERE latLong IS NOT NULL AND language = 'Hindi'
AND (date(timestamp_seconds(safe_cast(substr(createdOn, 1, 10) as int64))) = '2021-09-21')
GROUP BY 1, 2
ORDER BY 3 DESC;
