CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_FeedIssue_24Sep`
AS (
with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/1fb57083_1729256150911_sc.png' thumbnailUrl, 
'Tried to call? We are back! 🚀' title, 'Call now 💌 continue talking!' text
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-09-24" AND 
(TIMESTAMP(a.time) BETWEEN ('2025-09-24 07:30:00 UTC') AND ('2025-09-24 10:30:00 UTC')) AND b.tenant = 'fz'
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
'AdhocRetool_FeedIssue_24Sep' as templateId,
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
