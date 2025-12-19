--task1
INSERT INTO `maximal-furnace-783.vibely_analytics.crmFlashSaleNotifications` 
WITH users AS (
SELECT DISTINCT CURRENT_DATE("Asia/Kolkata") date, 
EXTRACT(HOUR FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata") hour, 
SAFE_CAST(a.distinct_id AS STRING) userId, b.phoneNo, b.name userName, 'FlashSale' state,

from `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-10-01" AND b.tenant = 'fz'),

templates AS (SELECT *, '' imageUrl, '' label, '' ctaText, 
TRUE showTimer, 60 timerValue, 'sticky' uiType 
FROM `maximal-furnace-783.vibely_analytics.crmNotificationTemplates`
WHERE state = 'FlashSale'),


matchedAll AS (
SELECT users.* , templates.*,
FROm users INNER JOIN templates ON templates.state = 'FlashSale'
),

final AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY RAND()) rnk,
case
when templateVariables='userName'then concat('{"userName":','"', userName,'"', '}')
when templateVariables = "hostName" then '{"hostName":"us"}'
when templateVariables = "userName, hostName" then  concat('{"userName":','"', userName,'"',',','"hostName":"us"','}')
else null
end as templateVariablesFinal
 FROM matchedAll
QUALIFY rnk = 1
)

SELECT SAFE_CAST(date AS TIMESTAMP) date, hour, userId, userName, phoneNo, target, '' category, '' as cohort,
templateId, title, text, thumbnailUrl,  imageUrl,  ctaText, label, templateVariablesFinal as templateVariables, 1 callsLifetime, 1 notificationsSent, 1 rowNumber, '1' notifRank, '1' setRank, '' appV, '' clientType, showTimer, timerValue, uiType  FROM final
