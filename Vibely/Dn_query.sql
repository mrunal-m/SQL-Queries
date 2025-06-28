-- Dn Query 
with
exp_base as(
select
distinct
date(TIMESTAMP,'Asia/Kolkata') as dt,
variant,
a.userId,
from `sharechat-production.experimentationV2.abTest_view_events_backendV2` a 
where expID = "88dede7d-8f78-4d19-b5b3-6c115a6737d1"
and DATE(TIMESTAMP,"Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(timestamp)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and version not in ('NA') and version > '0'
),


ho as (
select distinct
date(time,'Asia/Kolkata') as dt,
cast(distinct_id as string) userId
from `maximal-furnace-783.vibely_analytics.home_opened` a
where date(time,'Asia/Kolkata')
between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and tenant='fz'
),


feed_landers as (
  SELECT distinct date(time,'Asia/Kolkata')as dt,split(userId,'U')[1]  userId
  FROM `maximal-furnace-783.vibely_analytics.vibely_chat_feed_response_event` 
  WHERE DATE(time,'Asia/Kolkata') 
  BETWEEN date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
  and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
),

feed_fetched as (
  SELECT distinct date(time,'Asia/Kolkata') as dt,split(userId,'U')[1]  userId
  FROM `maximal-furnace-783.vibely_analytics.vibely_chat_feed_response_event` 
  WHERE DATE(time,'Asia/Kolkata') 
  BETWEEN date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")  
  and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
  and fetchStatus=true
),

feed_visitors as (
  select distinct
  DATE(time,'Asia/Kolkata') as dt,
  split(userId,'U')[1] userId
  from `maximal-furnace-783.vibely_analytics.vibely_chat_feed_item_view_event`
  where DATE(time,'Asia/Kolkata')
  between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
  and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
),

feed_clickers as (
  select distinct
  DATE(time,'Asia/Kolkata') as dt,
  split(userId,'U')[1] userId,
  from `maximal-furnace-783.vibely_analytics.vibely_fz_feed_click_events`
  where DATE(time,'Asia/Kolkata')
  between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
  and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
),

feed_quality_dau as (
select distinct dt,userId from (
SELECT distinct date(time,'Asia/Kolkata') dt,split(userId,'U')[1] userId
FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2`
WHERE DATE(time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
union all

select
distinct
date(time,'Asia/Kolkata') as dt,
split(userId,'U')[1] as userId,
from `maximal-furnace-783.vibely_analytics.dn_recharge_screen_event`
where DATE(time,'Asia/Kolkata')
between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action ='view'
union all
-- widget add and interstial click event

SELECT 
distinct date(time,'Asia/Kolkata') dt,
substr(userId,2) as userId,
FROM `maximal-furnace-783.vibely_analytics.interstitials_event`
where DATE(time,"Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action='primary_click'
and id not like ('%product_5_1_sc_consultation_audio%')


union all 

select distinct date(time,'Asia/Kolkata') as dt,
substr(userId,2) as userId
from `maximal-furnace-783.vibely_analytics.feed_widget_event`
where DATE(time,"Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action='click'
and id not in ('132', '123', '124', '125', '126', '127', '128', '129', '130', '131', '122')
)
),

dn_view as(
select
distinct
date(time,'Asia/Kolkata') as dt,
split(userId,'U')[1] as userId,
from `maximal-furnace-783.vibely_analytics.dn_recharge_screen_event`
where DATE(time,'Asia/Kolkata')
between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action ='view'
),

recharge_intent as(
select distinct dt,userId from (
SELECT 
distinct date(time,'Asia/Kolkata') dt,
substr(userId,2) as userId,
FROM `maximal-furnace-783.vibely_analytics.interstitials_event`
where DATE(time,"Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action='primary_click'
and id not like ('%product_5_1_sc_consultation_audio%')

union all

select distinct date(time,'Asia/Kolkata') as dt,
substr(userId,2) as userId
from `maximal-furnace-783.vibely_analytics.feed_widget_event`
where DATE(time,"Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action='click'
and id not in ('132', '123', '124', '125', '126', '127', '128', '129', '130', '131', '122')

union all

SELECT
distinct
date(a.time,'Asia/Kolkata') as dt,
a.userId as userId,
FROM `maximal-furnace-783.vibely_analytics.recharge_initiation_events` a
WHERE date(a.time, "Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action in ('CUSTOM_WEBVIEW_SELECTED_TURBO','BUY_COIN_CLICK')

union all

select
distinct
date(time,'Asia/Kolkata') as dt,
split(userId,'U')[1] as userId,
from `maximal-furnace-783.vibely_analytics.dn_recharge_screen_event`
where DATE(time,'Asia/Kolkata')
between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action in ('rechargeClicked','turboChange')

union all

select DATE(time,'Asia/Kolkata') as dt,
split(userId,'U')[1] as userId, from `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2`
where DATE(time,'Asia/Kolkata')
between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and action ='rechargeClicked'
)
),

pg_sel as(
SELECT distinct 
DATE(time,'Asia/Kolkata') as dt,
trim(userid,'U') userid,
FROM `maximal-furnace-783.vibely_analytics.recharge_initiation_events`
WHERE DATE(time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC') 
and  action in ('CUSTOM_WEBVIEW_SELECTED','GOOGLE_SELECTED','CUSTOM_WEBVIEW_SELECTED_TURBO')
and package not in ('100','500')
),

pp_open as(
SELECT distinct  
DATE(time,'Asia/Kolkata') as dt,
trim(userid,'U') userid,
FROM `maximal-furnace-783.vibely_analytics.sc_open_customized_payments_page` 
WHERE DATE(time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC') 
and status='payment_page_landed'
and inr_value not in (1,5)
),

pm_clicked as(
SELECT distinct  
DATE(time,'Asia/Kolkata') as dt,
trim(userid,'U') userid,
FROM `maximal-furnace-783.vibely_analytics.sc_recharge_wallet` 
WHERE DATE(time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC') 
and inr_value not in (1,5)
),

pm_clicked_with_orderId as(
SELECT distinct 
DATE(time,'Asia/Kolkata') as dt, 
trim(userid,'U') userid,
FROM `maximal-furnace-783.vibely_analytics.sc_recharge_wallet` 
WHERE DATE(time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC') 
and transactionId is not null
and inr_value not in (1,5)
),

recharge_initiated as(
SELECT distinct DATE(a.time,'Asia/Kolkata') dt,
a.userId userId
FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` a
WHERE DATE(a.time,'Asia/Kolkata')between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and lower(a.status) = "initiated"
and cost not in (1,5)
),


recharge_completed as(
SELECT distinct  DATE(a.time,'Asia/Kolkata') dt,
a.userId userId
FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` a
WHERE DATE(a.time,'Asia/Kolkata') between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and lower(a.status) = "success"
and cost not in (1,5)
),

calls as (
select *
from
(select *,
timestamp_diff(timestamp(session_ended_at), timestamp(session_started_at), SECOND)/60.0 as total_call_time,
date(time,'Asia/Kolkata') dt,
row_number() over (partition by consultation_id, vendor_session_id, status order by rowIngestionTime) as rn
from `maximal-furnace-783.sc_analytics.consultation` a
where date(time, "Asia/Kolkata") between date("2025-06-26",'Asia/Kolkata') and current_date("Asia/Kolkata")
and timestamp(time)>=TIMESTAMP('2025-06-26 10:30:00 UTC')
and consultation_type = 'FIND_A_FRIEND'
and tenant = "fz"
)
where rn = 1
),

extends as (
(select distinct dt,time, consultation_id as call_id, consultee_user_id as userId,discounted_fee_per_minute, discounted_max_minutes,
total_charges-5*discounted_fee_per_minute as total_charges,
total_call_time-discounted_max_minutes as total_call_time,
from calls
where
(
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
and status = 'completed')
),

paid_calls as (
select distinct dt,time, consultation_id as call_id, consultee_user_id as userId,discounted_fee_per_minute, discounted_max_minutes, total_charges, total_call_time,
from calls
where (discounted_fee_per_minute is null or discounted_fee_per_minute > 1)
and discounted_max_minutes is null
and total_charges > 0 and status = 'completed'),

all_pus as(
select
dt,
userId,
count(distinct call_id) calls,
sum(total_charges)/3.5 as GMV,
sum(total_call_time) PCT
from (select * from paid_calls union all select * from extends)
group by 1,2
)

select
a.dt,
a.variant,
count(distinct a.userId) as exp_users,
count(distinct b.userId) as ho,
count(distinct x.userId) as feed_landers,
count(distinct y.userId) as feed_fetched,
count(distinct c.userId) as feed_visitors,
count(distinct t.userId) as feed_clickers,
count(distinct d.userId) as feed_quality_dau,
count(distinct dn.userId) as dn_view,
count(distinct e.userId) as recharge_intent,

count(distinct p.userId) as pg_sel,
count(distinct q.userId) as pp_open,
count(distinct r.userId) as pm_clicked,
count(distinct s.userId) as pm_clicked_with_orderId,

count(distinct f.userId) as recharge_initiated,
count(distinct g.userId) as recharge_completed,
count(distinct h.userId) as PU,
sum(h.calls) as PU_calls,
sum(h.GMV) as PU_GMV,
sum(h.PCT) as PU_PCT,
from exp_base a 
left join ho b
on a.dt=b.dt and a.userId=b.userId
left join feed_landers x
on a.dt=x.dt and a.userId=x.userId
left join feed_fetched y
on a.dt=y.dt and a.userId=y.userId
left join feed_visitors c
on a.dt=c.dt and a.userId=c.userId
left join feed_clickers t
on a.userId=t.userId and a.dt=t.dt
left join feed_quality_dau d
on a.userId=d.userId and a.dt=d.dt
left join dn_view dn
on a.dt=dn.dt and a.userId=dn.userId
left join recharge_intent e
on a.userId=e.userId and a.dt=e.dt
left join pg_sel p 
on a.dt=p.dt and a.userId=p.userid
left join pp_open q  
on a.dt=q.dt and a.userId=q.userid
left join pm_clicked r
on a.dt=r.dt and a.userId=r.userid
left join pm_clicked_with_orderId s
on a.dt=s.dt and a.userId=s.userid
left join recharge_initiated f
on a.userId=f.userId
and a.dt=f.dt
left join recharge_completed g
on a.userId=g.userId
and a.dt=g.dt
left join all_pus h
on a.userId=h.userId
and a.dt=h.dt
group by 1,2
order by 1,2;
