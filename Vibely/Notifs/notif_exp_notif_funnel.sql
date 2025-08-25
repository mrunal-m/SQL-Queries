--realtime Notifs behav nudges CTR
WITH
exp_base AS (
SELECT DISTINCT date(TIMESTAMP,'Asia/Kolkata') as dt, variant, userId,
from `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
where expID = "ff9d0250-aacc-4af0-86ff-d45926dff5fd"
and DATE(TIMESTAMP,"Asia/Kolkata") >= "2025-08-22"
and timestamp(timestamp)>=TIMESTAMP('2025-08-22 07:30:00 UTC')
and version not in ('NA') and version > '0'
),

 t1 AS  (SELECT date(time, "Asia/Kolkata") dt, distinct_id userId, communityNotifId,  
CASE WHEN (communityNotifId like '%rechargeDropOff%') OR (communityNotifId LIKE "%system_canceled%" ) 
OR (communityNotifId LIKE "%no_answer%") OR (communityNotifId LIKE  "%canceled%") THEN 'DropOffNotif'
ELSE 'Others'END NotifType,
FROM `maximal-furnace-783.vibely_analytics.notification_initiated`
WHERE DATE(time, "Asia/Kolkata") >= "2025-08-22" AND status = 'init'
) ,

t2 AS (SELECT distinct_id userId, communityNotifId FROM `maximal-furnace-783.vibely_analytics.notification_issued`
WHERE DATE(time, "Asia/Kolkata") >= "2025-08-22"
 ),

t3 AS (SELECT time, distinct_id userId, communityNotifId FROM `maximal-furnace-783.vibely_analytics.notification_clicked`
WHERE DATE(time, "Asia/Kolkata") >= "2025-08-22"
 ),


count_base as(        
  select consultation_id,        
  max(safe_cast(consultation_count as int64)) as call_no,
  max(session_id) as sessionId,
  max(request_id) as requestId        
  from `maximal-furnace-783.sc_analytics.consultation`        
  where DATE(time,'Asia/Kolkata')>= "2025-08-20"
  and consultation_type = 'FIND_A_FRIEND' 
  and tenant = "fz" 
  group by 1        
),

calls as(
  select *
  from 
  (select a.*,b.call_no,b.requestId,b.sessionId,
  timestamp_diff(timestamp(session_ended_at), timestamp(session_started_at), SECOND)/60.0 as total_call_time,
  date(time,'Asia/Kolkata') dt,
  row_number() over (partition by a.consultation_id, vendor_session_id, status order by rowIngestionTime) as rn
  from `maximal-furnace-783.sc_analytics.consultation` a join count_base b on a.consultation_id=b.consultation_id
  where DATE(time,'Asia/Kolkata') >= "2025-08-20"
  and consultation_type = 'FIND_A_FRIEND' 
  and tenant = "fz" 
  )
  where rn = 1
),

other_paid_calls as (
  select dt, time, consultation_id as call_id,consultant_id, consultee_user_id as user_id, sessionId, --language, 
  discounted_fee_per_minute, discounted_max_minutes, total_charges, total_call_time call_time
  from calls
  where (discounted_fee_per_minute is null or discounted_fee_per_minute > 1)
  and discounted_max_minutes is null
  and total_charges >= 0
  and status = 'completed'
),


all_paid_calls as (
  select distinct dt,time, user_id,consultant_id, call_id,total_charges,call_time, sessionId from (select * from other_paid_calls)
)

SELECT  t1.dt,  NotifType, COUNT(DISTINCT exp_base.userId) expUsers, COUNT(DISTINCT t1.userId)initiatedUusers, 
COUNT(DISTINCT t1.communityNotifId) NotifInitiated, COUNT(DISTINCT t2.communityNotifId) NotifIssued,
COUNT(DISTINCT t3.communityNotifId) NotifClicked,
count(distinct ap.user_Id) PU 
FROM exp_base LEFT JOIN t1 ON exp_base.dt = t1.dt AND exp_base.userId = SAFE_CAST(t1.userId AS STRING)
LEFT JOIN t2 ON t1.communityNotifId = t2.communityNotifId
LEFT JOIN t3 ON t2.communityNotifId = t3.communityNotifId
LEFT JOIN all_paid_calls ap ON SAFE_CAST(t3.userId AS STRING) = ap.user_Id AND ap.time >= t3.time
AND ap.time BETWEEN t3.time AND TIMESTAMP_ADD(t3.time, INTERVAL 600 SECOND)
GROUP BY ALL
