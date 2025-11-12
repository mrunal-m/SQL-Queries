CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_IconsRecovery12Nov_2`
AS (
with a as (
WITH t1 AS (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/20b55d35_1759326008612_sc.png' thumbnailUrl, 
'Icons are online and waiting for you! 💖' title, 'Tap to call your fav Icon! ⭐' text
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-10-15" AND b.tenant = 'fz'),
t2 AS (
WITH count_base as(        
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
)


SELECT *, consultee_user_id userId FROM calls
where status = 'completed'
and call_no>=10
-- and consultation_id not in (select * from welcome_calls)
) SELECT t1.* FROM t1 INNER JOIN t2 ON t1.userId=t2.userId
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,
a.userName,
a.phoneNo,
callsLifetime,
notificationsSent,
'icons_feed' as target,
'adHoc' cohort,
'AdhocRetoolIPL_IconsRecovery12Nov_2' as templateId,
a.title, 
a.text, 
a.thumbnailUrl, 
notifRank, 
setRank,
setMap,
a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,row_number() over(partition by a.userId order by rand())rn 
from b left join a on true
--left join `maximal-furnace-783.vibely_analytics.crmNotificationTemplates` t on t.state='offer' and t.templateId like '%tier_drop%'
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)
