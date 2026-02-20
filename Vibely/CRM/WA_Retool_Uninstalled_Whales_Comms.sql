--- create a clone table & insert
--- things to replace: base CTE, templateId, cdnUrls

INSERT INTO `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_uninstalledWhales20Feb`
--vibely_whales_reactivation_2
(
select date, 
 CASE 
    WHEN COUNT(CASE WHEN cohort = 'video' THEN 1 END) > 0 THEN 'videoCall'
    ELSE MIN(cohort) END as cohort, language, UserId, phoneNo, userName, template, templateVariables, cdnUrl
from (

with base as (
with t1 AS (with
base1 AS (
SELECT
distinct dt,
REPLACE(LOWER(customer_user_id), '-', '') as deviceId,
app_version,
time,
bundle_id, advertising_id,
FROM (
SELECT DATE(time,'Asia/Kolkata') AS dt,
time,
customer_user_id,
app_version,
bundle_id, advertising_id,
ROW_NUMBER() OVER (PARTITION BY customer_user_id ORDER BY time desc) AS w
FROM (
SELECT time,app_version,customer_user_id,media_source,bundle_id, advertising_id
FROM `maximal-furnace-783.vibely_analytics.appsflyer_install`
WHERE date(time,'Asia/Kolkata') between '2024-10-01' and CURRENT_DATE('Asia/Kolkata')
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT time,app_version,customer_user_id,media_source,bundle_id, advertising_id
FROM `maximal-furnace-783.vibely_analytics.appsflyer_reinstall`
WHERE date(time,'Asia/Kolkata') between '2024-10-01' and CURRENT_DATE('Asia/Kolkata')
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT time,app_version,customer_user_id,media_source,bundle_id, advertising_id
FROM `maximal-furnace-783.vibely_analytics.appsflyer_reattribution`
WHERE date(time,'Asia/Kolkata') between '2024-10-01' and CURRENT_DATE('Asia/Kolkata')
GROUP BY 1,2,3,4,5,6
) AS a
)
where w = 1
AND dt <= CURRENT_DATE('Asia/Kolkata')
-- between date('2025-10-16','Asia/Kolkata') and CURRENT_DATE('Asia/Kolkata')
-- and datetime(time,'Asia/Kolkata') >= '2025-10-16 17:45:00'
and (lower(bundle_id) not like '%in.mohalla.friends.app%' or bundle_id is null)
-- and app_version >= '2025.026.01'
),

uninstall_base as(
SELECT distinct date(a.time,'Asia/Kolkata') as dt, a.advertising_id, b.deviceId  
FROM `maximal-furnace-783.vibely_analytics.appsflyer_uninstall` a
left join base1 b on a.advertising_id=b.advertising_id 
WHERE date(a.time,'Asia/Kolkata') between date('2026-01-10','Asia/Kolkata') and CURRENT_DATE('Asia/Kolkata')
and b.deviceId is not null
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
SELECT *, SAFE_CAST(consultee_user_id AS INT64)userId, consultee_device_id as deviceid FROM calls
where status = 'completed'
and call_no>=20
-- and consultation_id not in (select * from welcome_calls)
),

unins AS (
  SELECT DISTINCT f.deviceId FROM final f INNER JOIN uninstall_base  u
ON f.deviceId = u.deviceId)--,

-- with usr AS (
  SELECT DISTINCT id distinct_id, ANY_VALUE(phoneNo) phoneNo
from  `maximal-furnace-783.vibely_analytics.user` u
INNER JOIN unins un ON u.deviceid = un.deviceId
WHERE LENGTH(phoneNo)<14
GROUP BY 1
--  )

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
ON t1.distinct_id = SAFE_CAST(final.userId AS STRING)
)


SELECT DISTINCT current_date("Asia/Kolkata") as date, "AdhocWHalesIos" as cohortFinal, "English" as language, SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, 
from users a-- LEFT JOIN complete_call ON SAFE_CAST(a.distinct_id AS STRING) = complete_call.userId
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = b.id 
WHERE b.tenant = 'fz' --AND complete_call.userId IS NULL
)

select distinct cast(date as timestamp) date,*except(date)from (
  select base.date, cohortFinal as cohort, base.*except(date,cohortFinal), 'vibely_whales_reactivation_2' template,
  -- case when templateVariables='userName' then 
  -- concat('{"body" : ["' ,userName,'"],"header" : ["',cdnLinks,'"]}') else
  concat('{"header" : ["',"https://cdn-sc-g.sharechat.com/33d5318_1c8/creation/28ac2d7f_1742408237069_sc.jpeg",'"]}')   as templateVariables
  , row_number() over (partition by UserId order by rand())rn,"https://cdn-sc-g.sharechat.com/33d5318_1c8/creation/28ac2d7f_1742408237069_sc.jpeg" as cdnUrl from base 
  --left join `maximal-furnace-783.vibely_analytics.crmTemplatesTemp` t 
  --on --base.language = t.language and 
  -- base.cohortFinal = t.Cohort
  where  phoneNo is not null
  )
where rn = 1 and phoneNo is not null and template LIKE "%vibely_whales_reactivation_2%"  
)
GROUP BY ALL)
