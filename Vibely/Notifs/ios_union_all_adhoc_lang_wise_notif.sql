CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_iosReclaimDAU3`
AS (
(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/8944406c_1749220088864_sc.webp' thumbnailUrl, 
'tina patel is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/385fe141-b6e2-4388-95ef-ed8c732b7442?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Gujarati')
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
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, 
a.text, 
a.thumbnailUrl, 
notifRank, 
setRank,
setMap,
a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)
 UNION ALL
-- Mal

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/a0c7451f_1749220560690_sc.webp' thumbnailUrl, 
'Divya Nair is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/52f89ba3-1873-4159-9554-85e0acc68232?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Malayalam')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Tamil

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/1c981eef_1749220607828_sc.webp' thumbnailUrl, 
'Meenakshi is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/14beb950-1c73-42a3-b5ca-72f19df45160?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Tamil')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Marathi

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/2ee2c9ed_1750687744440_sc.webp' thumbnailUrl, 
'Tanvi is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/b0aea3a5-dd66-4571-9fd5-eaea3113bd5f?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Marathi')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Hindi

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/3b4ab5fe_1754721776535_sc.webp' thumbnailUrl, 
'piya is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/82c174ee-aec8-4065-adfa-58edd9cc496a?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Hindi')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Telugu 

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/0a88cc5d_1749220392706_sc.webp' thumbnailUrl, 
'priyanka is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/fb54fa7b-51dd-4724-b37b-20c5e68ff287?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Telugu')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Kannada 

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/405f0771_1749220350671_sc.webp' thumbnailUrl, 
'Prameela is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/f8fd5937-1066-42f7-b136-04259e06fe77?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Kannada')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Odia 

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/1bfc0065_1753847331983_sc.webp' thumbnailUrl, 
'Miki is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/fe6a4124-608a-4c89-91ae-df4afef71c77?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Odia')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Bengali 

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/1491c2ac_1749220995732_sc.webp' thumbnailUrl, 
'Sumita Das is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/2b00ec69-2416-4c6f-9966-22152a62f317?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Bengali')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)

UNION ALL
--Punjabi 

(with a as (
SELECT distinct SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, b.language,
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/vibely/d959b383_1749220800986_sc.webp' thumbnailUrl, 
'Tim Tim is ready to talk!' title, 'Just click and call 📞' text,
'https://vibely.co.in/call/2351e257-6840-47e2-95ab-d262a9037b57?mediaType=AUDIO' deeplink
from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user` b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-06-15" AND b.tenant = 'fz'
AND LOWER(a.clientType) LIKE "%ios%"  AND b.language IN ('Punjabi')
),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,a.userName,a.phoneNo,callsLifetime,notificationsSent,
'deeplink' as target,
'adHoc' cohort,
'AdhocRetool_iosReclaimDAU4' as templateId,
a.title, a.text, a.thumbnailUrl, notifRank, setRank,setMap,a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,a.deeplink,
row_number() over(partition by a.userId order by rand())rn 

from b left join a on true
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
))
