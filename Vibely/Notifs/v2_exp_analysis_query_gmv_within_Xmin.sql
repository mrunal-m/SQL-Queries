WITH exp_base as 
(SELECT variant, userId FROM `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
WHERE expID =  "34d5ebb3-3c64-49c4-a770-aa4e64e29846" AND 
DATE(timestamp,"Asia/Kolkata") >= "2025-06-10"
AND version NOT IN ('NA') AND version > '0'),

t1 AS 
(SELECT time, SAFE_CAST(distinct_id AS STRING) userId, communityNotifId, CASE 
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='VIDEO' THEN 'realtimeVideo'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='AUDIO' THEN 'realtimeAudio'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime'  THEN 'realtime'
  ELSE "others" END as type, 
FROM `maximal-furnace-783.vibely_analytics.notification_initiated`
WHERE date(time, "Asia/Kolkata") = CURRENT_DATE("Asia/Kolkata") - 1
AND status = 'init'
AND LOWER(clientType)  LIKE "%android%"),

t2 AS 
(SELECT time, SAFE_CAST(distinct_id AS STRING) userId, communityNotifId, CASE 
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='VIDEO' THEN 'realtimeVideo'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='AUDIO' THEN 'realtimeAudio'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime'  THEN 'realtime'
  ELSE "others" END as type, 
FROM `maximal-furnace-783.vibely_analytics.notification_issued`
WHERE date(time, "Asia/Kolkata") = CURRENT_DATE("Asia/Kolkata") - 1
AND LOWER(clientType)  LIKE "%android%"
GROUP BY ALL),

t3 AS 
(SELECT time, SAFE_CAST(distinct_id AS STRING) userId, communityNotifId, CASE 
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='VIDEO' THEN 'realtimeVideo'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='AUDIO' THEN 'realtimeAudio'
  WHEN SPLIT(communityNotifId, '/')[0]='fz_realtime'  THEN 'realtime'
  ELSE "others" END as type, 
FROM `maximal-furnace-783.vibely_analytics.notification_clicked`
WHERE date(time, "Asia/Kolkata") = CURRENT_DATE("Asia/Kolkata") - 1
AND LOWER(clientType)  LIKE "%android%"
GROUP BY ALL),


cons AS (select time, consultee_user_id userId, consultation_id callId, total_charges  
from `maximal-furnace-783.sc_analytics.consultation`        
  WHERE date(time, "Asia/Kolkata") = CURRENT_DATE("Asia/Kolkata") - 1
  and consultation_type = 'FIND_A_FRIEND' AND status = 'completed'
  and tenant = "fz" )

SELECT exp_base.variant, date(t1.time, "Asia/Kolkata") dt,
t1.type, --date(t1.time, "Asia/Kolkata") dt,
COUNT(DISTINCT t1.userId) usersInitiated,
COUNT(DISTINCT t2.userId) usersIssued,
COUNT(DISTINCT t3.userId) usersClicked,

COUNT(DISTINCT t1.communityNotifId) notifInitiated,
COUNT(DISTINCT t2.communityNotifId) notifIssued,
COUNT(DISTINCT t3.communityNotifId) notifClicked,

COUNT(DISTINCT cons.userId) PU,
COUNT(DISTINCT cons.callId) calls,
SUM(cons.total_charges)/3.5 GMV


FROM exp_base
LEFT JOIN t1 ON exp_base.userId = t1.userId
LEFT JOIN t2 ON t1.userId = t2.userId AND t1.communityNotifId = t2.communityNotifId 
LEFT JOIN t3 ON t2.userId = t3.userId AND t2.communityNotifId = t3.communityNotifId
LEFT JOIN cons ON t3.userId = cons.userId AND cons.time BETWEEN t3.time AND TIMESTAMP_ADD(t3.time, INTERVAL 600 SECOND)

WHERE t1.type IS NOT NULL

GROUP BY ALL
