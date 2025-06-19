--select count(distinct userId) from maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_IPL_7Apr
CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.AdhocRetool_RechargeOffer_d3`
AS 
(
with a as (
SELECT *, 'Pay less, talk more 💬' title, 'Up to 25% off on recharges' text, 
'https://bb.branding-element.com/prod/98064/98064-25_Off.jpg' thumbNailUrl ,
'English' language  FROM `maximal-furnace-783.Sourabh.recharge_offer_notif_exp_base`
WHERE group1 = 'variant_1'

),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
SAFE_CAST(a.userId AS STRING) userId,
a.userName,
a.phoneNo,
callsLifetime,
notificationsSent,
'wallet' as target,
'adHoc' cohort,
'AdhocRetool_RechargeOffer_template5' as templateId,
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
