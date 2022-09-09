SELECT date(time, "Asia/Kolkata") as date, TIMESTAMP_TRUNC(time, HOUR, "Asia/Kolkata") as hr, 
SUM(duration) as duration, SUM(SAFE_CAST(timeSpent AS INT64)) as TS
from maximal-furnace-783.moj_analytics.video_play as vp
WHERE date(time, "Asia/Kolkata") BETWEEN "2022-08-22" AND "2022-08-24"
AND vp.percentageFloat>=0
        and vp.percentageFloat<=100
        and vp.duration <= 6000
        and vp.duration is not null
        and vp.duration > 0
        and (safe_cast(vp.timespent as int64)/1000 - (vp.repeatCount + vp.percentageFloat/100) * vp.duration) > 0
GROUP BY 1, 2
ORDER BY 1, 2
