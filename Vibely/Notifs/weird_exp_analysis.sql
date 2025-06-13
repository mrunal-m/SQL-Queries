WITH exp_base as 
(SELECT variant, userId FROM `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
WHERE expID =  "b7bb0aeb-fef0-4573-a8ab-e002fa312008" AND 
DATE(timestamp,"Asia/Kolkata") >= "2025-06-10"
AND version NOT IN ('NA') AND version > '0'),

t1 AS 
(SELECT time, SAFE_CAST(distinct_id AS STRING) userId, communityNotifId, 
CASE WHEN communityNotifId LIKE "%control&et=off%" THEN "control"
  WHEN communityNotifId LIKE '%v=variant-1%' THEN "v1" 
  WHEN communityNotifId LIKE '%v=variant-2%' THEN "v2" 
  WHEN (communityNotifId LIKE '%v=variant-2%' AND communityNotifId LIKE '%b=true%') THEN "weird" 
  ELSE "weird2" END as type, 
FROM `maximal-furnace-783.vibely_analytics.notification_initiated`
WHERE date(time, "Asia/Kolkata") = "2025-06-13" AND SPLIT(communityNotifId, '/')[0]='fz_realtime' AND SPLIT(communityNotifId, '/')[4] ='AUDIO'
AND status = 'init'
AND LOWER(clientType)  LIKE "%android%")

SELECT variant, type, exp_base.userId, t1.userId, t1.communityNotifId communityNotifId
FROM exp_base INNER JOIN t1 ON exp_base.userId = t1.userId
WHERE type NOT LIKE "%control%" AND variant = 'control'
