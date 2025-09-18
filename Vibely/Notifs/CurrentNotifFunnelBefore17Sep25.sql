create temp function start_date() as (DATE_SUB(current_date('Asia/Kolkata'), INTERVAL 3 DAY));
create temp function end_date() as (DATE_SUB(current_date('Asia/Kolkata'), INTERVAL 1 DAY));

delete from maximal-furnace-783.vibely_analytics.crmNotificationsAnalyticsFunnel
where date between start_date() and end_date();
insert into maximal-furnace-783.vibely_analytics.crmNotificationsAnalyticsFunnel
 
with sent as(
  select date(date) date,
  if(cohort like '%call%','call',cohort)cohort, 
  count(distinct userID) as users 
  from `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplate`
  where date(date) between start_date() and end_date()
  group by 1,2
  UNION ALL
  select date(date), 
  "1stRechargeNoCall" cohort, 
  count(distinct userId) as users 
  from `maximal-furnace-783.vibely_analytics.crmRechargeNoWCWithTemplateLog`
  where date(date) between start_date() and end_date()
  group by 1, 2
),

users as (
  select
  distinct id,
  deviceId,
  language,
  from `maximal-furnace-783.sc_analytics.user`
  where tenant = "fz"
),

sent_f as(
  select date,
  cohort, 
  sum(users)users from sent
  group by 1,2
),

a as(
  select distinct date(time,'Asia/Kolkata') as date, LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID from `maximal-furnace-783.vibely_analytics.notification_initiated`
  where date(time,'Asia/Kolkata') between start_date() and end_date()
  and type like '%fz%' and (status is null or status = 'init')
),

b as(
  SELECT distinct date(time,'Asia/Kolkata')date, LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] as target,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  FROM `maximal-furnace-783.vibely_analytics.notification_issued`
  Where date(time,'Asia/Kolkata') between start_date() and end_date() 
  and type like '%fz%'
),

c as(
  select distinct date(time,'Asia/Kolkata')date , LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  from `maximal-furnace-783.vibely_analytics.notification_clicked`
  where date(time,'Asia/Kolkata') between start_date() and end_date() 
  and type like '%fz%'
) ,

fin as(
  select a.date,
  a.state, 
  count(distinct a.id)initiated,
  count(distinct a.userid) initiatedUsers, 
  count(distinct b.id) issues, 
  count(distinct b.userid) issuedUsers, 
  count(distinct c.id)clicks, 
  count(distinct c.userId) clickedUsers
  from a 
  left join b
  on a.date = b.date and a.id = b.id 
  left join c
  on a.date = c.date and a.id =c.id
  group by 1,2
),

ho as (
  select distinct date(time,'Asia/Kolkata')date, cast(distinct_id as string) userid,
  IF(if(referrer like '%notifications_fz%',split(referrer,'_')[3],null) LIKE "%call%", split(referrer,'_')[5], if(referrer like '%notifications_fz%',split(referrer,'_')[3],null)) id, 
  sessionId 
  from `maximal-furnace-783.vibely_analytics.home_opened`
  where date(time,'Asia/Kolkata') between start_date() and end_date()
  AND tenant = 'fz'
),

feed as (
  select distinct
  DATE(time,'Asia/Kolkata') date,
  split(userid,'U')[1] userid, 
  sessionId
  FROM `maximal-furnace-783.vibely_analytics.vibely_chat_feed_item_view_event`
  WHERE DATE(time,'Asia/Kolkata') between start_date() and end_date()
),


qdau as (
  SELECT DISTINCT * from (
  select distinct DATE(time, 'Asia/Kolkata') date,
  split(userId,'U')[1] userId, sessionId 
  from `maximal-furnace-783.vibely_analytics.first_call_experience`
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
  and screen in ("fullScreen","bottomScreen",'fullScreenAutoCall','fullScreenInterstitial')

  union all
  SELECT distinct date(time,'Asia/Kolkata') date,split(userId,'U')[1] userId,sessionID
  FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2` ap 
  WHERE DATE(time,'Asia/Kolkata') between start_date() and end_date()

  UNION ALL

  select
  distinct 
  date(time,'Asia/Kolkata') date,split(userId,'U')[1] userId,sessionID 
  from `maximal-furnace-783.vibely_analytics.dn_recharge_screen_event`
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
  and action ='view'
  union all 

  SELECT 
  distinct 
  date(time,'Asia/Kolkata') date,split(userId,'U')[1] userId,sessionID 
  FROM `maximal-furnace-783.vibely_analytics.interstitials_event`
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
  and action='primary_click'

  union all 

  select distinct 
  date(time,'Asia/Kolkata') date,split(userId,'U')[1] userId,sessionID 
  from `maximal-furnace-783.vibely_analytics.feed_widget_event`
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
  and action='click'
)
),

count_base as(        
  select consultation_id,        
  max(safe_cast(consultation_count as int64)) as call_no,
  max(session_id) as sessionId,
  max(request_id) as requestId        
  from `maximal-furnace-783.sc_analytics.consultation`        
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
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
  where DATE(time,'Asia/Kolkata') between start_date() and end_date()
  and consultation_type = 'FIND_A_FRIEND' 
  and tenant = "fz" 
  )
  where rn = 1
),

welcome_calls as(
  select dt, time, consultation_id as call_id, consultee_user_id as user_id,discounted_fee_per_minute, discounted_max_minutes, sessionId,
  case
  when discounted_fee_per_minute = 0 and discounted_max_minutes = 6 then 0
  when discounted_fee_per_minute = 5 and discounted_max_minutes = 5  then least(total_charges,25)  
  when discounted_fee_per_minute = 1 and discounted_max_minutes = 5 then least(total_charges,5) 
  end as total_charges, 

  case
  when discounted_fee_per_minute = 0 and discounted_max_minutes = 6 then least(total_call_time,6) 
  when discounted_fee_per_minute = 5 and discounted_max_minutes = 5  then  least(total_call_time,5)
  when discounted_fee_per_minute = 1 and discounted_max_minutes = 5  then  least(total_call_time,5)
  end as total_call_time, 
  from calls
  where ((discounted_fee_per_minute = 5 and discounted_max_minutes = 5) or (discounted_fee_per_minute=0 and discounted_max_minutes = 6)
  or (discounted_fee_per_minute = 1 and discounted_max_minutes = 5)) and status ='completed' 
),


-- 1 Re call converted to Paid call
extended_paid_calls as (
  select dt, time, consultation_id as call_id,consultant_id, consultee_user_id as user_id, sessionId,
  discounted_fee_per_minute, discounted_max_minutes, total_charges, total_call_time call_time
  from calls
  where (
    (discounted_fee_per_minute = 5
  and discounted_max_minutes = 5
  and total_charges > 25) 
  or
  (discounted_fee_per_minute = 0
  and discounted_max_minutes = 6
  and total_charges > 0)
  or 
  (discounted_fee_per_minute = 1
  and discounted_max_minutes = 5
  and total_charges > 5) 
  )
  and status='completed'
),

-- 2nd call onwards
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
  select distinct dt,time, user_id,consultant_id, call_id,total_charges,call_time, sessionId from (select * from extended_paid_calls union all select * from other_paid_calls)
),

base as(select date,if(state like '%call%','call',state)state, 
count(distinct aid)notificationsInitiated, 
count(distinct bid) notificationsDelivered, 
count(distinct cid) notificationsClicked, 
count(distinct auserId)usersInitiated, 
count(distinct buserid) delivered, 
count(distinct cuserid) clicked, 
count(distinct houserID) ho, 
count(distinct fuserId) feed, 
count(distinct quserid)qDau,
count(distinct wc_uid) wc_callers,
count(distinct apUserId)PU,
sum(if(rn=1,total_charges,0))/3.5 as total_call_gmv,
sum(if(rn=1,call_time,0)) as total_call_time, clientType from (
select 
a.date,a.state, a.clientType, a.id aid, b.id as bid, c.id as cid,a.userid as auserid,b.userid as buserid, c.userid as cuserid, ho.userid as hoUserid,f.userid as fUserId,
q.userid as qUserid, ap.user_id as apUserId, ap.total_charges, ap.call_time,
wc.user_id as wc_uid,
row_number() over (partition by a.date,a.userId,ap.call_id order by ap.time)rn
from a left join b on 
a.date = b.date and a.userid = b.userid and a.id = b.id and a.clientType = b.clientType
left join c on 
a.date = c.date and a.userid = c.userid and a.id = c.id and a.clientType = c.clientType
left join ho on
a.date = ho.date and cast(a.userid as string) =ho.userid and a.id = ho.id
left join feed f on ho.date = f.date and ho.userid = f.userid and ho.sessionId = f.sessionId
left join qDau q on ho.date = q.date and ho.userid =q.userId and ho.sessionId = q.sessionID
left join welcome_calls wc on ho.date = wc.dt and ho.userid =wc.user_id and ho.sessionId = wc.sessionID
left join all_paid_calls ap on ho.date=ap.dt and ho.userid = ap.user_id AND ho.sessionId = ap.sessionId
)
group by date, state, clientType
order by 1,2)

select b.* except(clientType), s.users as targetedUsers, clientType   from base b left join sent s on b.date = s.date and b.state =s.cohort;



delete from maximal-furnace-783.vibely_analytics.crmTemplateAnalytics
where date between start_date() and end_date();
insert into maximal-furnace-783.vibely_analytics.crmTemplateAnalytics 


with a as(
  select distinct date(time,'Asia/Kolkata')date, LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  SPLIT(communityNotifId, '/')[3] copy,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID from `maximal-furnace-783.vibely_analytics.notification_initiated`
  where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%' and (status is null or status = 'init')
),

b as(
  SELECT distinct date(time,'Asia/Kolkata')date, LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] as target,
  SPLIT(communityNotifId, '/')[3] copy,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  FROM `maximal-furnace-783.vibely_analytics.notification_issued`
  Where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%'
),

c as(
  select distinct  date(time,'Asia/Kolkata')date, LOWER(clientType) clientType,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%icon%" then 'iconsOnboarding'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification_milestone%" then 'gamification'
  WHEN LOWER(communityNotifId) LIKE "%fz_star_gamification_milestone%" then 'starGamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  SPLIT(communityNotifId, '/')[3] copy,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  from `maximal-furnace-783.vibely_analytics.notification_clicked`
  where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%'
),

f as( select * from `maximal-furnace-783.vibely_analytics.crmNotificationTemplates`),

base as(select a.date,if(a.state like '%call%','call',a.state)state, a.copy, a.clientType,
count(distinct a.id)initiated, count(distinct b.id) issued, count(distinct c.id) clicked from a
left join b on a.date= b.date and a.id = b.id and a.copy = b.copy and a.clientType = b.clientType
left join c on a.date= c.date and a.id = c.id and a.copy = c.copy and a.clientType = b.clientType
group by 1,2,3, 4
order by 1,2,3) 
select base.* except(clientType),f.title,f.text, clientType from base left join f on base.copy= f.templateId
