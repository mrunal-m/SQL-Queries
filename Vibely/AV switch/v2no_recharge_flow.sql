-- no recharge flow
WITH 
av_base AS (SELECT DISTINCT  SUBSTRING(av.userId, 2) userId, callId, action, screen, mediaType, avSwitchEligibleId, avSwitchReqId,
FROM `maximal-furnace-783.vibely_analytics.call_av_switch_event` av 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC')),

v2_base AS (SELECT DISTINCT SUBSTRING(v2.userId, 2) userId, callId, mediaType, action, screen, meta
FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2` v2 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC') AND hostId IS NOT NULL
),

switchClick AS (
SELECT DISTINCT callId FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked') AND avSwitchEligibleId IS NOT NULL
),

eligibleReqMade AS (
SELECT DISTINCT callId, FROM av_base av 
WHERE  mediaType = 'audio' AND action IN ('videoEligibleRequestMade') AND avSwitchEligibleId IS NOT NULL
),

eligibleReqSuccess AS (
SELECT DISTINCT callId, FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoEligibleRequestSuccess') AND avSwitchEligibleId IS NOT NULL
),

rechargePopup AS (
SELECT DISTINCT callId, FROM  v2_base 
WHERE mediaType = 'audio' AND screen = 'rechargePopup' AND action = 'view'
AND LENGTH(JSON_VALUE(meta, '$.avSwitchEligibleId'))>1
),

switchNoRechargePopUp AS (
SELECT distinct s.callId from switchClick s
LEFT JOIN rechargePopup r ON s.callId = r.callId
WHERE r.callId IS NULL),


videoReqMade AS (
SELECT DISTINCT callId FROM av_base av 
WHERE  mediaType = 'audio' AND action IN ('videoRequestMade') AND avSwitchEligibleId IS NOT NULL
),

videoReqSuccess AS (
SELECT DISTINCT callId FROM av_base av
WHERE mediaType = 'audio' AND action IN ('videoRequestSuccess') AND avSwitchReqId IS NOT NULL
),

videoSwitchScreen AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'videoCallSwitchScreen' --AND avSwitchReqId IS NOT NULL
),

userCancelled AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'videoRequestCancel' AND avSwitchReqId IS NOT NULL
),

HostDeclined AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'videoRequestNotAcceptedNudge' --AND avSwitchReqId IS NOT NULL
),

videoConnected AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'videoConnected' AND mediaType = 'video'
),

ConsAV AS (
SELECT DISTINCT  consultee_user_id userId, consultation_id callId, COUNT(distinct CASE WHEN media_type IS NOT NULL THEN media_type END) mediaCount 
FROM `maximal-furnace-783.sc_analytics.consultation` 
WHERE date(time, "Asia/Kolkata") >= "2025-06-04" AND timestamp(time)>=TIMESTAMP('2025-06-04 06:30:00 UTC')
GROUP BY 1, 2 
HAVING mediaCount >1
)

SELECT DISTINCT
COUNT(DISTINCT switchClick.callId) switchClicked,
COUNT(DISTINCT eligibleReqMade.callId) eligibleReqMade,
COUNT(DISTINCT eligibleReqSuccess.callId) eligibleReqSuccess,
COUNT(DISTINCT switchNoRechargePopUp.callId) rechargePopup,
COUNT(DISTINCT videoReqMade.callId) videoReqMade, 
COUNT(DISTINCT videoReqSuccess.callId) videoReqSuccess,
COUNT(DISTINCT videoSwitchScreen.callId) videoSwitchScreen,
COUNT(DISTINCT userCancelled.callId) userCancelled,
COUNT(DISTINCT HostDeclined.callId) HostDeclined,
COUNT(DISTINCT videoConnected.callId) videoConnected,
COUNT(DISTINCT ConsAV.callId) ConsAV


FROM switchClick 
LEFT JOIN eligibleReqMade ON switchClick.callId = eligibleReqMade.callId
LEFT JOIN eligibleReqSuccess ON eligibleReqMade.callId = eligibleReqSuccess.callId
LEFT JOIN switchNoRechargePopUp ON eligibleReqSuccess.callId = switchNoRechargePopUp.callId
LEFT JOIN videoReqMade ON switchNoRechargePopUp.callId = videoReqMade.callId
LEFT JOIN videoReqSuccess ON videoReqMade.callId = videoReqSuccess.callId
LEFT JOIN videoSwitchScreen ON videoReqSuccess.callId = videoSwitchScreen.callId
LEFT JOIN userCancelled ON videoSwitchScreen.callId = userCancelled.callId
LEFT JOIN HostDeclined ON videoSwitchScreen.callId = HostDeclined.callId
LEFT JOIN videoConnected ON videoSwitchScreen.callId = videoConnected.callId
LEFT JOIN ConsAV ON videoSwitchScreen.callId = ConsAV.callId
