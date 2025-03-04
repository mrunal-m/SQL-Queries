With vibely_users AS (SELECT distinct Id, phoneNo FROM `maximal-furnace-783.sc_analytics.user` WHERE (tenant = 'fz'and length(phoneNo)<=14)),

fz_users AS (SELECT distinct Id, phoneNo FROM `maximal-furnace-783.sc_analytics.user` WHERE (tenant IS NULL OR tenant = 'sc') and length(phoneNo)<=14),

fz_to_vibely AS (SELECT vibely_users.Id, fz_users.Id fzId FROM vibely_users INNER JOIN fz_users ON vibely_users.phoneNo = fz_users.phoneNo),

fz_PUs_to_vibely as (SELECT distinct date(time, "Asia/Kolkata") dt, Id FZuserID, b.Id vibelyId FROM `maximal-furnace-783.sc_analytics.consultation` a 
  left join fz_to_vibely b on a.consultee_user_id=b.fzId
  where date(time, "Asia/Kolkata") >= "2024-10-01"
  and consultation_type = 'FIND_A_FRIEND'
  and (tenant = 'sc' or tenant is null)
  and status = 'completed'
  and total_charges>0
),

-- user_type AS (SELECT Id, 
-- CASE WHEN Id IN (SELECT Id from fz_to_vibely) AND Id IN (SELECT Id from sc_pu) THEN 'FZ_to_vibely'
-- ELSE 'vibely_user' END as user_type  from maximal-furnace-783.sc_analytics.user
-- WHERE tenant = 'fz'
-- GROUP BY 1, 2),

calls AS (select * FROM (SELECT *, TIMESTAMP_DIFF(timestamp(session_ended_at), timestamp(session_started_at), SECOND)/60.0 as total_call_time,
date(time,'Asia/Kolkata') dt,
row_number() over (partition by consultation_id, vendor_session_id, status order by rowIngestionTime) as rn
from `maximal-furnace-783.sc_analytics.consultation` a 
where date(time, "Asia/Kolkata") >= "2024-11-01"
and consultation_type = 'FIND_A_FRIEND'
and tenant = "fz"
)
where rn = 1

),

first_calls as(
  select dt, time, consultation_id as call_id, consultee_user_id as distinct_id,discounted_fee_per_minute, discounted_max_minutes, 
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
or (discounted_fee_per_minute = 1 and discounted_max_minutes = 5)) and status ='completed' ),

first_callers as (
select dt, distinct_id userId, count(distinct call_id) welcomeCalls, sum(total_charges)/5 as GMV, sum(total_call_time) PCT from 
first_calls
group by 1,2
),

extends as (
  (select distinct dt, consultation_id as call_id, consultee_user_id as userId,discounted_fee_per_minute, discounted_max_minutes, 
  total_charges-5*discounted_fee_per_minute total_charges,  
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
 and  status = 'completed')
),


extended_paid_calls as ( 
select dt, userID, count(distinct call_id) calls, sum(total_charges)/3.5 as GMV, sum(total_call_time) PCT 
from extends
 group by 1,2
),

paid_calls as (SELECT dt, userId, COUNT(distinct call_id) calls, sum(total_charges)/3.5 as GMV, sum(total_call_time) PCT
from (select distinct dt, consultation_id as call_id, consultee_user_id as userId,--discounted_fee_per_minute, discounted_max_minutes, 
total_charges, total_call_time, from calls
where (discounted_fee_per_minute is null or discounted_fee_per_minute > 5)
and discounted_max_minutes is null
and total_charges >= 0 and  status = 'completed')
GROUP BY 1, 2),


all_calls AS
(SELECT  dt, userId, SUM(calls) calls, sum(GMV) as GMV, sum(PCT) PCT
FROM (SELECT * from paid_calls
UNION ALL
SELECT * from extended_paid_calls
UNION ALL
SELECT * from first_callers)
GROUP BY 1, 2)

SELECT dt, user_type, SUM(calls) calls,  COUNT(DISTINCT userId) callers, sum(GMV) as GMV, sum(PCT) PCT  FROM 
(
SELECT all_calls.dt, all_calls.userId,
CASE WHEN fz_PUs_to_vibely.vibelyID IS NOT NULL THEN 'FZ_to_vibely'
ELSE 'vibely_user' END as user_type,
all_calls.calls, all_calls.gmv, all_calls.PCT, 
ROW_NUMBER() OVER(PARTITION BY all_calls.userId,all_calls.dt ORDER BY fz_PUs_to_vibely.dt DESC) as row_no,
from all_calls LEFT JOIN fz_PUs_to_vibely ON all_calls.userId = fz_PUs_to_vibely.vibelyId AND --(all_calls.dt = fz_PUs_to_vibely.dt)
date_diff(all_calls.dt,fz_PUs_to_vibely.dt,day)<=30
)
WHERE row_no = 1
GROUP BY 1, 2
ORDER BY 1
