--task2
INSERT INTO `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplate` 
WITH users AS (SELECT * FROM `maximal-furnace-783.askk_analytics.crmOnboardingGenericBase`
WHERE date = CURRENT_DATE("Asia/Kolkata")
AND hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata")),

templates AS (SELECT * FROM `maximal-furnace-783.askk_analytics.crmNotificationTemplates`
WHERE state = 'onboardingGeneric'),

copy_cycle AS (SELECT DISTINCT SAFE_CAST(distinct_id AS STRING) userId, type,
SPLIT(communityNotifId, '/')[SAFE_OFFSET(3)] templateId 
from maximal-furnace-783.askk_analytics.notification_issued
WHERE DATE(time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") - 3
AND type= 'askk_onboarding'),

matchedAll AS (
SELECT users.*, templates.*,
FROm users LEFT JOIN templates ON templates.state = 'onboardingGeneric'
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

SELECT SAFE_CAST(date AS TIMESTAMP) date, hour, userId, userName, phoneNo, target, '' category, 'onboardingGeneric' cohort,
 templateId, title, text, thumbnailUrl, imageUrl, ctaText, label, templateVariablesFinal as templateVariables, 1 callsLifetime, 1 notificationsSent, 1 rowNumber, '1' notifRank, '1' setRank, '' appV, '' clientType FROM deduped
