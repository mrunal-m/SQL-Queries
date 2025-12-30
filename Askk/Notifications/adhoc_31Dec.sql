--task2
TRUNCATE TABLE `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplateFinal` ;
INSERT INTO `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplateFinal` 

WITH users AS (SELECT DISTINCT CURRENT_DATE("Asia/Kolkata") date, 16 hour, 
SAFE_CAST(distinct_id AS STRING) userId, '' userName, '' phoneNo, '31_Dec' state
 FROM `maximal-furnace-783.askk_analytics.home_opened` WHERE DATE(time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") - 30),

templates AS (SELECT * FROM `maximal-furnace-783.askk_analytics.crmNotificationTemplates`
WHERE state = 'Old_Year'),

copy_cycle AS (SELECT DISTINCT SAFE_CAST(distinct_id AS STRING) userId, type,
SPLIT(communityNotifId, '/')[SAFE_OFFSET(3)] templateId 
from maximal-furnace-783.askk_analytics.notification_issued
WHERE DATE(time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") - 3
AND type= 'askk_onboarding'),

matchedAll AS (
SELECT users.*, templates.*,
FROm users INNER JOIN templates ON templates.state = 'Old_Year'
),

deduped AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY RAND()) rnk,
case
when templateVariables='userName'then concat('{"userName":','"', userName,'"', '}')
when templateVariables = "hostName" then '{"hostName":"us"}'
when templateVariables = "userName, hostName" then  concat('{"userName":','"', userName,'"',',','"hostName":"us"','}')
else null
end as templateVariablesFinal
 FROM 
(SELECT matchedAll.* FROM matchedAll LEFT JOIN copy_cycle 
ON matchedAll.userId = copy_cycle.userId AND matchedAll.templateId = copy_cycle.templateId
WHERE copy_cycle.userId IS NULL AND copy_cycle.templateId IS NULL)
QUALIFY rnk = 1
)

SELECT SAFE_CAST(date AS TIMESTAMP) date, hour, userId, userName, phoneNo, target, '' category, 'Adhoc_31Dec' cohort,
 templateId, title, text, thumbnailUrl, imageUrl, ctaText, label, templateVariablesFinal as templateVariables, 1 callsLifetime, 1 notificationsSent, 1 rowNumber, '1' notifRank, '1' setRank, '' appV, '' clientType FROM deduped
