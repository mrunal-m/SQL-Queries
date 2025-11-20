with -- Sound Dn till date v2
exp_base as (
select distinct variant, userId,
min(date(TIMESTAMP,'Asia/Kolkata')) as dt,
min(datetime(timestamp,'Asia/Kolkata')) as dt_time
from `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
where expID = "1f24052c-126a-4d70-b758-4163f2489ace"
and DATE(TIMESTAMP,"Asia/Kolkata") >= '2025-11-11'
and version not in ('NA') and version > '0'
group by 1,2
),

user_type AS (SELECT userId, 
CASE WHEN min_dt >= "2025-11-11" THEN 'newUser'
ELSE 'oldUser' END userType,
FROM (SELECT DISTINCT SAFE_CAST(distinct_id AS STRING) userId, 
DATE(MIN(time), "Asia/Kolkata") min_dt
-- ROW_NUMBER() OVER(PARTITION BY distinct_id ORDER BY time ASC) rn
FROM `maximal-furnace-783.vibely_analytics.home_opened`
WHERE DATE(time, "Asia/Kolkata")>= "2024-10-01"
-- QUALIFY rn = 1
GROUP BY ALL
)
),

variant_users as(
select variant, userType, count(distinct e.userId) as exp_users
from exp_base e inner join user_type u ON e.userId = u.userId
group by all
),

count_base as(
SELECT consultation_id as call_id,max(consultation_count) as call_no,
max(safe_cast(client_type as string)) as client_type,
FROM `maximal-furnace-783.sc_analytics.consultation` 
WHERE date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
group by 1 
),

calls as 
(select *
from 
(select a.*, cast(c.call_no as int64) as call_no, c.client_type as clientType,
timestamp_diff(timestamp(session_ended_at), timestamp(session_started_at), SECOND)/60.0 as total_call_time,
date(time,'Asia/Kolkata') dt,datetime(time,'Asia/Kolkata') as dt_time,
row_number() over (partition by a.consultation_id, vendor_session_id, status order by rowIngestionTime) as rn
from `maximal-furnace-783.sc_analytics.consultation` a  join count_base c on c.call_id = a.consultation_id
WHERE date(time,'Asia/Kolkata') >= '2025-11-11'
and datetime(time,'Asia/Kolkata') >= '2025-11-11'
and consultation_type = 'FIND_A_FRIEND'
and tenant = "fz"
)
where rn = 1
and status = "completed"
),

recharge_completed as(
SELECT e.variant, u.userType, sum(cost) as recharge_gmv_rs, sum(units) as recharge_gmv_coins, count(distinct transactionId) as total_recharges
FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` a join exp_base e
on a.distinct_id = e.userId and datetime(time,'Asia/Kolkata') >= e.dt_time
join user_type u ON  a.distinct_id = u.userId
WHERE date(time,'Asia/Kolkata') >= '2025-11-11'
and datetime(time,'Asia/Kolkata') >= '2025-11-11 10:00:00'
and lower(a.status) = "success"
group by all
)

-- select * from recharge_completed


select distinct a.variant, a.userType, v.exp_users, a.callers as total_callers,a.calls as total_calls, a.gmv_rs as total_gmv_rs, a.pct as total_call_time,
pu, pu_calls, pu_call_time, pu_gmv_rs,
r.recharge_gmv_rs, r.recharge_gmv_coins
from
(
select e.variant, user_type.userType,
count(distinct consultee_user_id) as callers, count(distinct consultation_id) as calls,
sum(total_charges)/3.5 as gmv_rs, sum(total_call_time) as pct,
count(distinct case when call_no > 1 then consultee_user_id end) as pu,
count(distinct case when call_no > 1 then consultation_id end) as pu_calls,
sum(case when call_no > 1 then total_call_time end) as pu_call_time, 
sum(case when call_no > 1 then total_charges end)/3.5 as pu_gmv_rs 
from calls c join exp_base e on c.consultee_user_id = e.userId
join user_type On e.userId = user_type.userId
and c.dt_time >= e.dt_time
group by all
) a left join variant_users v on a.variant = v.variant AND  a.userType = v.userType
left join recharge_completed r on a.variant = r.variant AND a.userType = r.usertype
order by 1

