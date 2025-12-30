CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_1Jan2026_gifter2`
AS 
(
with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/4a1aec4_1767100402391_sc.png' thumbnailUrl, 
'Someone special? 😍' title, 'Wish them with a gift! ✨' text
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-10-01" AND b.tenant = 'fz'
AND b.id IN (select distinct source_user_id from `maximal-furnace-783.sc_analytics.chatroom_transaction_ledger`
where date(time,'Asia/Kolkata') >= '2025-10-01' AND source_currency = "COIN"
           AND target_currency = "GEM"
           AND operation_type = "TRANSFER"
           AND entity in ('CONSULTATION_GIFTING', 'FZ_CONSULTATION_GIFTING','VIDEO_CONSULTATION_GIFTING')
)
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
'AdhocRetool_1Jan2026_gifters2' as templateId,
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
