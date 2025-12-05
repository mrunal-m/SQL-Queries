with t1 AS (SELECT DISTINCT distinct_id
from  `maximal-furnace-783.vibely_analytics.home_opened`
WHERE DATE(time, 'Asia/Kolkata') = current_date('Asia/Kolkata') 
AND timestamp(time) >= TIMESTAMP('2025-11-01 00:00:00 UTC') 
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

final as (
SELECT *, SAFE_CAST(consultee_user_id AS INT64)userId FROM calls
where status = 'completed'
and call_no>=20
-- and consultation_id not in (select * from welcome_calls)
),
 
users AS (
SELECT distinct t1.distinct_id 
from t1 INNER JOIN final
ON t1.distinct_id = final.userId
-- INNER JOIN incomplete_call ON final.userId = SAFE_CAST(incomplete_call.userId AS INT64)
)


SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
from users a-- LEFT JOIN complete_call ON SAFE_CAST(a.distinct_id AS STRING) = complete_call.userId
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE b.tenant = 'fz' --AND complete_call.userId IS NULL

