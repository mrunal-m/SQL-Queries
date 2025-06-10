WITH exp_base AS
(SELECT DISTINCT variant, userId FROM `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
WHERE expID  IN ("34d5ebb3-3c64-49c4-a770-aa4e64e29846", "b7bb0aeb-fef0-4573-a8ab-e002fa312008") AND 
DATE(timestamp,"Asia/Kolkata") >= "2025-06-10"
AND version NOT IN ('NA') AND version > '0'),

t1 AS 
(SELECT date(time, "Asia/Kolkata") dt, distinct_id, communityNotifId, --appVersion,
from `maximal-furnace-783.vibely_analytics.notification_initiated`
WHERE date(time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") - 5
AND status = 'init'
AND LOWER(clientType)  LIKE "%android%"
GROUP BY ALL)

SELECT t1.dt, exp_base.variant, 
COUNT(distinct t1.distinct_id) usersInitiated, COUNT(distinct t1.communityNotifId) notifInitiated,
COUNT(DISTINCT case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then distinct_id END) UsersrealtimeAudioInit,
COUNT(DISTINCT case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then communityNotifId END) realtimeAudioInit,
COUNT(DISTINCT case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then distinct_id END) UsersrealtimeVideoInit,
COUNT(DISTINCT case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then communityNotifId END) realtimeVideoInit
FROM exp_base INNER JOIN t1 ON exp_base.userId = SAFE_CAST(t1.distinct_id AS STRING)
GROUP BY ALL
ORDER BY 1
