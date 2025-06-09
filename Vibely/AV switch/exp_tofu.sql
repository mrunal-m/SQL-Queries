-- checking exp variant wise top of the funnel
WITH 
exp_base AS 
(SELECT DISTINCT userId, variant FROM `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
WHERE expID =  "9141cd90-998c-4cbf-af86-b8d9580ba381" AND 
DATE(timestamp,"Asia/Kolkata") >= "2025-06-04"
AND timestamp(timestamp)>=TIMESTAMP('2025-06-04 06:30:00 UTC')
AND version NOT IN ('NA') AND version > '0'),

t1 AS (SELECT DISTINCT consultee_user_id userId, consultant_user_id hostId, consultation_id, media_type
FROM `maximal-furnace-783.sc_analytics.consultation`
WHERE consultation_type = 'FIND_A_FRIEND' AND tenant = 'fz' AND status = 'completed'
AND date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC')
),

t2 AS (
SELECT DISTINCT userId, hostId,  consultation_id,
FROM t1 WHERE LOWER(media_type) LIKE "%audio%"),

host AS 
(SELECT DISTINCT SAFE_CAST(distinct_id AS STRING) hostId, MAX(appVersion) appV 
FROM `maximal-furnace-783.sc_analytics.home_opened` h 
INNER JOIN `sc-bigquery-product-analyst.FZ_Video_Supply.fz_video_supply` 
ON SAFE_CAST(h.distinct_id AS STRING) = userId
WHERE date(time, "Asia/Kolkata") >= "2025-05-01"
GROUP BY 1  HAVING appV >= 251301),

t3 AS (
SELECT t2.userId, t2.hostId, t2.consultation_id FROM 
t2 INNER JOIN host ON t2.hostId = host.hostId),

switchClick AS (
SELECT DISTINCT  SUBSTRING(av.userId, 2) userId, callId, 
FROM `maximal-furnace-783.vibely_analytics.call_av_switch_event` av 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC')
AND mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
)

SELECT exp_base.variant, 
COUNT(distinct exp_base.userId) expUsers, 
COUNT(DISTINCT t1.userId) Callers,
COUNT(DISTINCT t1.consultation_id) TotalCalls,
COUNT(DISTINCT t2.consultation_id) AudioCalls,
COUNT(DISTINCT t3.consultation_id) AudioCallsWithEligibleHosts,
COUNT(DISTINCT switchClick.callId) switchClicked

FROM exp_base 

LEFT JOIN t1 ON exp_base.userId = t1.userId
LEFT JOIN t2 ON t1.userId = t2.userId AND t1.consultation_id = t2.consultation_id
LEFT JOIN t3 ON t2.hostId = t3.hostId AND t2.consultation_id = t3.consultation_id
LEFT JOIN switchClick ON t3.consultation_id = switchClick.callid
GROUP BY ALL
