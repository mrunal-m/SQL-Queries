with t1 AS (SELECT DISTINCT distinct_id
from  `maximal-furnace-783.vibely_analytics.home_opened`
WHERE DATE(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') -1
AND timestamp(time)>=TIMESTAMP('2025-04-19 19:30:00 UTC') AND timestamp(time)<=TIMESTAMP('2025-04-20 03:30:00 UTC') 
AND tenant = 'fz'
),


count_base as(        
  select consultee_user_id userId, consultation_id,        
  max(safe_cast(consultation_count as int64)) as call_no        
  from `maximal-furnace-783.sc_analytics.consultation`        
  where date(time, "Asia/Kolkata") >= "2024-10-01"
  and consultation_type = 'FIND_A_FRIEND'
  and tenant = "fz" 
  group by all     
),

calls as(
select *
from
(select a.*,c.call_no,
timestamp_diff(timestamp(session_ended_at), timestamp(session_started_at), SECOND)/60.0 as total_call_time,
date(time,'Asia/Kolkata') dt,
row_number() over (partition by a.consultation_id, vendor_session_id, status order by rowIngestionTime) as rn
from `maximal-furnace-783.sc_analytics.consultation` a 
join count_base c 
on a.consultation_id=c.consultation_id 
where date(time, "Asia/Kolkata") >= "2024-10-01"
and consultation_type = 'FIND_A_FRIEND'
and tenant = "fz"
)
where rn = 1
),
welcome_calls as (
(select distinct consultation_id 
from calls
where
(
(discounted_fee_per_minute = 5
and discounted_max_minutes = 5)
or
(discounted_fee_per_minute = 0
and discounted_max_minutes = 6)
or
(discounted_fee_per_minute = 1
and discounted_max_minutes = 5)
)
and status = 'completed')
),


final as (
SELECT *, SAFE_CAST(consultee_user_id AS INT64)userId FROM calls
where status = 'completed'
and call_no>=20
-- and consultation_id not in (select * from welcome_calls)
)


SELECT distinct t1.distinct_id from t1 INNER JOIN final
ON t1.distinct_id = final.userId
