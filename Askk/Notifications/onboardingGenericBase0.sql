WITH splash AS (
SELECT DISTINCT SAFE_CAST(_eventMeta.userProperties.userId AS STRING) userId, _eventMeta.processingProperties.tenant tenant, _eventMeta.appProperties.clientType clientType, _eventMeta.appProperties.appVersion appV FROM `maximal-furnace-783.askk_analytics.splash_screen_open`
WHERE DATE(time, "Asia/Kolkata") >= "2025-12-01"
),

user AS (SELECT DISTINCT id userId, name userName, phoneNo from  `maximal-furnace-783.sc_analytics.user`
WHERE tenant = 'askk'),

call10 AS (SELECT DISTINCT consultee_user_id userId, client_type, MAX(SAFE_CAST(consultation_count AS INTEGER)) as call_no 
FROM `maximal-furnace-783.askk_analytics.consultation`
WHERE DATE(time, "Asia/Kolkata") >= "2025-12-01" AND tenant = 'askk'
GROUP BY ALL
HAVING call_no <=100
)

SELECT DISTINCT 
CURRENT_DATE( "Asia/Kolkata") date, EXTRACT(HOUR FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata") hr, 
splash.userId, user.userName, user.phoneNo,
CASE WHEN (splash.userId IS NOT NULL AND call10.userId IS NULL) THEN 'splash'
WHEN (splash.userId IS NOT NULL AND call10.userId IS NOT NULL) THEN 'caller'
ELSE NULL END as state,
FROM splash LEFT JOIN user On splash.userId = user.userId
LEFT JOIN call10 On splash.userId = call10.userId
ORDER BY 1
