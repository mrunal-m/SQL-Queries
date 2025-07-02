WITH a AS (SELECT DATE(date) dt, cohort, template, COUNT(DISTINCT userId) targetUsers
FROM `maximal-furnace-783.vibely_analytics.crmDataWithTemplate` 
WHERE date(date) >= "2025-06-10"
GROUP BY ALL),

WA AS (
with t1 AS (select date(time) dt, messageId, eventUuid, recipientPhoneNo 
from `maximal-furnace-783.vibely_analytics.engati_webhook_responses` t1
WHERE date(t1.time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
AND statusMessage = 'sent'
GROUP BY ALL),


t2 AS (select distinct date(time,'Asia/Kolkata') dt, JSON_EXTRACT_SCALAR(data, "$.data.templateName") AS templateName,
JSON_EXTRACT_SCALAR(data, "$.response.responseObject.message_id") messageID, phoneNo
FROM `maximal-furnace-783.sc_analytics.respond_api_events`
where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
),

sent AS (
SELECT t1.dt, t2.templateName, COUNT(distinct t1.messageId) sent from t1 
INNER JOIN t2 ON (t1.messageId = t2.messageID) AND  (t1.recipientPhoneNo = t2.phoneNo)
--WHERE templateName LIKE '%vibely%'
GROUP BY ALL),

t3 AS (select date(time) dt, messageId, eventUuid, recipientPhoneNo 
from `maximal-furnace-783.vibely_analytics.engati_webhook_responses`
WHERE date(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
AND statusMessage = 'delivered'
GROUP BY ALL),


t4 AS (select distinct date(time,'Asia/Kolkata') dt, JSON_EXTRACT_SCALAR(data, "$.data.templateName") AS templateName,
JSON_EXTRACT_SCALAR(data, "$.response.responseObject.message_id") messageID, phoneNo
FROM `maximal-furnace-783.sc_analytics.respond_api_events`
where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
),

delivered AS (
SELECT t3.dt, t4.templateName, COUNT(distinct t3.messageId) delivered from t3 
INNER JOIN t4 ON (t3.messageId = t4.messageID) AND  (t3.recipientPhoneNo = t4.phoneNo)
--WHERE templateName LIKE '%vibely%'
GROUP BY ALL),



t5 AS (select date(time) dt, messageId, eventUuid, recipientPhoneNo 
from `maximal-furnace-783.vibely_analytics.engati_webhook_responses`
WHERE date(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
AND statusMessage = 'failed'
GROUP BY ALL),


t6 AS (select distinct date(time,'Asia/Kolkata') dt, JSON_EXTRACT_SCALAR(data, "$.data.templateName") AS templateName,
JSON_EXTRACT_SCALAR(data, "$.response.responseObject.message_id") messageID, phoneNo
FROM `maximal-furnace-783.sc_analytics.respond_api_events`
where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
),

failed AS (
SELECT t5.dt, t6.templateName, COUNT(distinct t5.messageId) failed from t5 
INNER JOIN t6 ON (t5.messageId = t6.messageID) AND  (t5.recipientPhoneNo = t6.phoneNo)
--WHERE templateName LIKE '%vibely%'
GROUP BY ALL),


t7 AS (select date(time) dt, messageId, eventUuid, recipientPhoneNo 
from `maximal-furnace-783.vibely_analytics.engati_webhook_responses`
WHERE date(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
AND statusMessage = 'read'
GROUP BY ALL),


t8 AS (select distinct date(time,'Asia/Kolkata') dt, JSON_EXTRACT_SCALAR(data, "$.data.templateName") AS templateName,
JSON_EXTRACT_SCALAR(data, "$.response.responseObject.message_id") messageID, phoneNo
FROM `maximal-furnace-783.sc_analytics.respond_api_events`
where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') - 7
),

read AS (
SELECT t7.dt,  t8.templateName, COUNT(distinct t7.messageId) read from t7 
INNER JOIN t8 ON (t7.messageId = t8.messageID) AND  (t7.recipientPhoneNo = t8.phoneNo)
--WHERE templateName LIKE '%vibely%'
GROUP BY ALL)

SELECT sent.dt,  --sent.templateName,
 sent.templateName,
 SUM(sent.sent) sent, SUM(delivered.delivered) delivered, SUM(failed.failed) failed, SUM(read.read) read FROM sent 
INNER JOIN delivered ON (sent.dt = delivered.dt) AND (sent.templateName=delivered.templateName)
INNER JOIN failed ON (delivered.dt = failed.dt) AND (delivered.templateName= failed.templateName)
INNER JOIN read ON (failed.dt = read.dt) AND ( failed.templateName =read.templateName)

GROUP BY ALL)

SELECT a.dt, a.cohort,  a.template,  a.targetUsers, SUM(WA.sent) sent, SUM(WA.delivered) delivered, SUM(WA.failed) failed
FROM a LEFT JOIN WA ON a.dt = WA.dt AND a.template = WA.templateName 
GROUP BY ALL

