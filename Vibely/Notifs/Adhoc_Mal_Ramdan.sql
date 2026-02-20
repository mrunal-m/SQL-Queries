CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_22FebAdhoc_Mal`
AS (
with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/2f0da8d3_1771590020912_sc.webp' thumbnailUrl, 
'A small pause for yourself 🤍' title, 'Share gifts & conversations!' text
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64) AND b.language IN ('Malayalam')
WHERE date(a.time, "Asia/Kolkata") >= "2025-12-01" AND b.tenant = 'fz'
AND SAFE_CAST(a.distinct_id AS STRING) NOT IN (SELECT DISTINCT consultant_user_id  FROM`maximal-furnace-783.sc_analytics.consultant` 
WHERE DATE(time, "Asia/Kolkata") >= "2025-12-01" AND tenant = 'fz')
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
'feed' as target,
'adHoc' cohort,
'AdhocRetool_22FebAdhoc_Mal' as templateId,
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
