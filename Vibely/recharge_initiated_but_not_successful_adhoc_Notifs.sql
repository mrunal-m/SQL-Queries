CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_UPI_downtime7Oct`
AS (
with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/1fb57083_1729256150911_sc.png' thumbnailUrl, 
'UPI on PhonePe, GPay & Paytm is Back! 🚀' title, 'Faced issues with payments? All fixed now. Start calling your friends!✅' text
from `maximal-furnace-783.vibely_analytics.home_opened` a 
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-04-16" AND b.tenant = 'fz'
AND SAFE_CAST(a.distinct_id AS STRING) IN ( WITH init AS (
  SELECT DISTINCT distinct_id FROM  `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` r
    WHERE LOWER(r.status) = 'initiated' AND DATE(time) = CURRENT_DATE() --AND timestamp(time)>=TIMESTAMP('2025-10-07 12:00:00 UTC')
      ), success as (
      SELECT DISTINCT distinct_id FROM  `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` r
    WHERE LOWER(r.status) = 'success' AND DATE(time) = CURRENT_DATE() --AND timestamp(time)>=TIMESTAMP('2025-10-07 12:00:00 UTC')
      ) SELECT init.distinct_id FROM  init LEFT JOIN success ON init.distinct_id = success.distinct_id
      WHERE success.distinct_id IS NULL
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
'AdhocRetool_UPI_downtime7Oct' as templateId,
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
