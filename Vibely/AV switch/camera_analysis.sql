--camera analysis
WITH av_base AS (SELECT DISTINCT  SUBSTRING(av.userId, 2) userId, callId, action, screen, mediaType, avSwitchEligibleId, avSwitchReqId, requestId,
cameraPermissionPresent, moderationPermissionRequired
FROM `maximal-furnace-783.vibely_analytics.call_av_switch_event` av 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC')),

v2_base AS (SELECT DISTINCT SUBSTRING(v2.userId, 2) userId, callId, mediaType, action, screen, meta, requestId, 
JSON_VALUE(meta, '$.avSwitchEligibleId')avSwitchEligibleId
FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2` v2 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC') AND hostId IS NOT NULL
),

switchClick AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
),

cameraPresent AS (
SELECT DISTINCT  callId, avSwitchEligibleId FROM av_base
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
AND cameraPermissionPresent = TRUE
),

cameraNeeded AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM av_base
WHERE action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
AND cameraPermissionPresent = FALSE AND callId NOT IN (SELECT distinct callId FROM cameraPresent)
),

-- cameraNeededModNeeded AS (
-- SELECT DISTINCT callId, avSwitchEligibleId FROM av_base
-- WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
-- AND cameraPermissionPresent = FALSE AND moderationPermissionRequired = TRUE
-- ),

modPermPresent AS (
SELECT callId, avSwitchEligibleId FROM av_base 
WHERE  mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
AND moderationPermissionRequired = FALSE
),

modPermNeeded AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM av_base 
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
AND moderationPermissionRequired = TRUE AND callId NOT IN (SELECT callId from modPermPresent)
),

sawWarning AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM v2_base
WHERE screen='video_moderation_screen' and action='view' ),

acceptWarning AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM v2_base
WHERE screen='video_moderation_screen' and action='accepted' ),

eligibleReqMade AS (
SELECT DISTINCT callId, avSwitchEligibleId FROM av_base av 
WHERE  mediaType = 'audio' AND action IN ('videoEligibleRequestMade') AND avSwitchEligibleId IS NOT NULL
),

sawCameraScreen AS (
SELECT DISTINCT SUBSTRING(v2_base.userId, 2) userId, callId, v2_base.requestId, c.requestId
FROM v2_base 
LEFT JOIN `maximal-furnace-783.vibely_analytics.vibely_permissions_event` c ON v2_base.requestid = c.requestId
WHERE c.action = 'view')

SELECT 
COUNT(DISTINCT switchClick.callId) switchClick,
COUNT(DISTINCT cameraNeeded.callId) cameraNeeded,
-- COUNT(DISTINCT cameraNeeded.callId) cameraNeeded,
-- COUNT(DISTINCT sawCameraScreen.callId) sawCameraScreen,
COUNT(DISTINCT modPermPresent.callId) modPermPresent,
COUNT(DISTINCT modPermNeeded.callId) modPermNeeded,
-- COUNT(DISTINCT cameraNeededModNeeded.callId) cameraNeededModNeeded,
COUNT(DISTINCT sawWarning.callId) sawWarning,
COUNT(DISTINCT acceptWarning.callId) acceptWarning,
COUNT(DISTINCT eligibleReqMade.callId) eligibleReqMade,


FROM switchClick
LEFT JOIN cameraNeeded ON switchClick.callId = cameraNeeded.callId AND switchClick.avSwitchEligibleId = cameraNeeded.avSwitchEligibleId
-- LEFT JOIN cameraPresent ON switchClick.callId = cameraPresent.callId AND switchClick.avSwitchEligibleId = cameraPresent.avSwitchEligibleId
LEFT JOIN modPermPresent ON cameraNeeded.callId = modPermPresent.callId AND  cameraNeeded.avSwitchEligibleId = modPermPresent.avSwitchEligibleId
LEFT JOIN modPermNeeded ON cameraNeeded.callId = modPermNeeded.callId AND cameraNeeded.avSwitchEligibleId = modPermNeeded.avSwitchEligibleId
LEFT JOIN sawWarning ON cameraNeeded.callId = sawWarning.callId AND  cameraNeeded.avSwitchEligibleId = cameraNeeded.avSwitchEligibleId
LEFT JOIN acceptWarning ON sawWarning.callId = acceptWarning.callId AND  sawWarning.avSwitchEligibleId = acceptWarning.avSwitchEligibleId

LEFT JOIN eligibleReqMade ON acceptWarning.callId = eligibleReqMade.callId AND acceptWarning.avSwitchEligibleId = eligibleReqMade.avSwitchEligibleId
-- LEFT JOIN modPermNeeded ON cameraPresent.callId = modPermNeeded.callId
-- LEFT JOIN acceptWarning ON modPermNeeded.callId = acceptWarning.callId
