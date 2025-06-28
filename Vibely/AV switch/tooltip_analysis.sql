WITH ho AS (SELECT SAFE_CAST(distinct_id AS STRING) userId, MAX(appVersion) appV
FROM  `maximal-furnace-783.vibely_analytics.home_opened`
WHERE DATE(time) >=CURRENT_DATE("Asia/Kolkata") -5
GROUP BY ALL
HAVING MAX(appVersion)>= 202501501
),

av AS (SELECT DATE(time, "Asia/Kolkata") dt, SUBSTRING(userId, 2) userId, 
COUNT(DISTINCT callId) total_AV_eligible_calls,
COUNT(DISTINCT CASE WHEN action = 'videoNudgeShown' THEN callId END) toolTipShown
-- callId, screen, action 
FROM `maximal-furnace-783.vibely_analytics.call_av_switch_event`
WHERE DATE(time) >=CURRENT_DATE("Asia/Kolkata") -5 
GROUP BY 1, 2),

av_agg AS (
SELECT av.dt, av.userId, av.total_AV_eligible_calls, av.toolTipShown 
FROM av INNER JOIN ho ON av.userId = ho.userId --AND av.dt = ho.dt
WHERE av.dt >= CURRENT_DATE("Asia/Kolkata") - 5
GROUP BY ALL)

SELECT toolTipShown, COUNT(DISTINCT userId) users
FROM av_agg
GROUP BY ALL
ORDER BY 2 DESC 


