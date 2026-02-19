--- create a clone table & insert
--- things to replace: base CTE, templateId, cdnUrls


INSERT INTO `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_ios19Feb` 

(
select date, 
 CASE 
    WHEN COUNT(CASE WHEN cohort = 'video' THEN 1 END) > 0 THEN 'videoCall'
    ELSE MIN(cohort) END as cohort, language, UserId, phoneNo, userName, template, templateVariables, cdnUrl
from (

with base as (with t1 AS (SELECT DISTINCT distinct_id
from  `maximal-furnace-783.vibely_analytics.home_opened`
WHERE DATE(time, 'Asia/Kolkata') = current_date('Asia/Kolkata') 
AND timestamp(time) >= TIMESTAMP('2026-01-15 00:00:00 UTC') 
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
),
 
users AS (
SELECT distinct t1.distinct_id 
from t1 INNER JOIN final
ON t1.distinct_id = final.userId
)


SELECT DISTINCT current_date("Asia/Kolkata") as date, "AdhocWHalesIos" as cohortFinal, "English" as language, SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, 
from users a-- LEFT JOIN complete_call ON SAFE_CAST(a.distinct_id AS STRING) = complete_call.userId
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE b.tenant = 'fz' --AND complete_call.userId IS NULL
)

select distinct cast(date as timestamp) date,*except(date)from (
  select base.date, cohortFinal as cohort, base.*except(date,cohortFinal), 'vibely_whales_reactivation' template,
  -- case when templateVariables='userName' then 
  -- concat('{"body" : ["' ,userName,'"],"header" : ["',cdnLinks,'"]}') else
  concat('{"header" : ["',"https://bb.branding-element.com/prod/128921/128921-09122025_130419-FZ_Comms_FRIENDS_The%20only%20theraphy%20you%E2%80%99ll%20ever%20need%20%283%29.png",'"]}')   as templateVariables
  , row_number() over (partition by UserId order by rand())rn,"https://bb.branding-element.com/prod/128921/128921-09122025_130419-FZ_Comms_FRIENDS_The%20only%20theraphy%20you%E2%80%99ll%20ever%20need%20%283%29.png" as cdnUrl from base 
  --left join `maximal-furnace-783.vibely_analytics.crmTemplatesTemp` t 
  --on --base.language = t.language and 
  -- base.cohortFinal = t.Cohort
  where  phoneNo is not null
  )
where rn = 1 and phoneNo is not null and template LIKE "%vibely_whales_reactivation%"  
)
GROUP BY ALL)
