-- combined funnel flow AV cancel v2
WITH 

exp_base AS (
SELECT DISTINCT date(TIMESTAMP,'Asia/Kolkata') as dt, variant, userId,
from `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
where expID = "d7ccdd26-a08c-423f-b772-cae8ee1c6fa9"
and DATE(TIMESTAMP,"Asia/Kolkata") >= "2025-08-20"
and timestamp(timestamp)>=TIMESTAMP('2025-08-20 06:50:00 UTC')
and version not in ('NA') and version > '0'
--GROUP BY ALL
),

av_base AS (SELECT time,  SUBSTRING(av.userId, 2) userId, callId, action, screen, mediaType, avSwitchEligibleId, avSwitchReqId,
FROM `maximal-furnace-783.vibely_analytics.call_av_switch_event` av 
WHERE date(time, "Asia/Kolkata") >= "2025-08-20" AND timestamp(time)>=TIMESTAMP('2025-08-20 06:50:00 UTC')),

v2_base AS (SELECT time, SUBSTRING(v2.userId, 2) userId, callId, mediaType, action, screen, meta
FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2` v2 
WHERE date(time, "Asia/Kolkata") >= "2025-08-20" AND timestamp(time)>=TIMESTAMP('2025-08-20 06:50:00 UTC') AND hostId IS NOT NULL
),

switchButtonView AS (
SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, time, userId, callId FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonView') 
),

actionSheetView AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, time, userId, callId FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('avBsView') 
),

switchClick AS (
SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, time, userId, callId FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoSwitchButtonClicked', 'avBsVideoCallClick') AND avSwitchEligibleId IS NOT NULL
),

eligibleReqMade AS (
SELECT DISTINCT callId, FROM av_base av 
WHERE  mediaType = 'audio' AND action IN ('videoEligibleRequestMade') AND avSwitchEligibleId IS NOT NULL
),

eligibleReqSuccess AS (
SELECT DISTINCT callId, FROM av_base av 
WHERE mediaType = 'audio' AND action IN ('videoEligibleRequestSuccess') AND avSwitchEligibleId IS NOT NULL
),


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

cancelClick AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'videoRequestCancelClicked' AND avSwitchReqId IS NOT NULL
),

cancelDcView AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'avCancelDcView' AND avSwitchReqId IS NOT NULL
),

DcWait AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'avCancelDcWaitClicked' AND avSwitchReqId IS NOT NULL
),

DcCancel AS (
SELECT DISTINCT callId FROM av_base av
WHERE screen = 'callScreen' AND action = 'avCancelDcCancelClicked' AND avSwitchReqId IS NOT NULL
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
WHERE date(time, "Asia/Kolkata") >= "2025-08-20" AND timestamp(time)>=TIMESTAMP('2025-08-20 06:50:00 UTC')
GROUP BY 1, 2 
HAVING mediaCount >1
)

SELECT exp_base.dt,
variant, 
COUNT(DISTINCT exp_base.userId) expUsers,
COUNT(DISTINCT switchButtonView.callId) switchButtonView,
COUNT(DISTINCT switchClick.callId) switchClicked,
COUNT(DISTINCT eligibleReqMade.callId) eligibleReqMade,
COUNT(DISTINCT eligibleReqSuccess.callId) eligibleReqSuccess,
-- COUNT(DISTINCT switchNoRechargePopUp.callId) switchNoRechargePopUp,
COUNT(DISTINCT videoReqMade.callId) videoReqMade, 
COUNT(DISTINCT videoReqSuccess.callId) videoReqSuccess,
COUNT(DISTINCT videoSwitchScreen.callId) videoSwitchScreen,
COUNT(DISTINCT cancelClick.callId) cancelClick, 
COUNT(DISTINCT cancelDcView.callId) cancelDcView, 
COUNT(DISTINCT DcWait.callId) DcWait, 
COUNT(DISTINCT DcCancel.callId) DcCancel, 
COUNT(DISTINCT userCancelled.callId) userCancelled,
COUNT(DISTINCT HostDeclined.callId) HostDeclined,
COUNT(DISTINCT videoConnected.callId) videoConnected,
COUNT(DISTINCT ConsAV.callId) ConsAV



FROM exp_base 
LEFT JOIN switchButtonView ON exp_base.userId = switchButtonView.userId AND exp_base.dt = switchButtonView.dt
LEFT JOIN switchClick ON switchButtonView.callId = switchClick.callId 

LEFT JOIN eligibleReqMade ON switchClick.callId = eligibleReqMade.callId
LEFT JOIN eligibleReqSuccess ON eligibleReqMade.callId = eligibleReqSuccess.callId
-- LEFT JOIN switchNoRechargePopUp ON eligibleReqSuccess.callId = switchNoRechargePopUp.callId
LEFT JOIN videoReqMade ON eligibleReqSuccess.callId = videoReqMade.callId
LEFT JOIN videoReqSuccess ON videoReqMade.callId = videoReqSuccess.callId
LEFT JOIN videoSwitchScreen ON videoReqSuccess.callId = videoSwitchScreen.callId
LEFT JOIN cancelClick ON videoSwitchScreen.callId = cancelClick.callId
LEFT JOIn cancelDcView ON cancelClick.callId = cancelDcView.callId
LEFT JOIN DcWait ON cancelDcView.callId = DcWait.callId 
LEFT JOIN DcCancel ON cancelDcView.callId = DcCancel.callId
LEFT JOIN userCancelled ON videoSwitchScreen.callId = userCancelled.callId
LEFT JOIN HostDeclined ON videoSwitchScreen.callId = HostDeclined.callId
LEFT JOIN videoConnected ON videoSwitchScreen.callId = videoConnected.callId
LEFT JOIN ConsAV ON videoSwitchScreen.callId = ConsAV.callId

GROUP BY ALL 
ORDER BY 2, 1 DESC
