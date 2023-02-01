-- Raw BQ Tables Event COUNT
(
SELECT 27 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-27" 
AND (time(time) >= time(datetime "2023-01-27 17:30:00.000000"))
)
UNION ALL
(
SELECT 28 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-28" 
AND (time(time) <= time(datetime "2023-01-28 07:10:00.000000"))
)
UNION ALL
(
SELECT 20 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-20" 
AND (time(time) >= time(datetime "2023-01-20 17:30:00.000000"))
)
UNION ALL
(
SELECT 21 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-21" 
AND (time(time) <= time(datetime "2023-01-21 07:10:00.000000"))
)
UNION ALL
(
SELECT 13 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-13" 
AND (time(time) >= time(datetime "2023-01-13 17:30:00.000000"))
)
UNION ALL
(
SELECT 14 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-14" 
AND (time(time) <= time(datetime "2023-01-14 07:10:00.000000"))
)
UNION ALL
(
SELECT 6 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-06" 
AND (time(time) >= time(datetime "2023-01-06 17:30:00.000000"))
)
UNION ALL
(
SELECT 7 as dt, COUNT(*) AS rawEventCount 
from `maximal-furnace-783.sc_analytics.post_like`
WHERE date(time) = "2023-01-07" 
AND (time(time) <= time(datetime "2023-01-07 07:10:00.000000"))
)
ORDER BY 1 ASC
