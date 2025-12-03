--task 1
INSERT INTO `maximal-furnace-783.askk_analytics.crmOnboardingGenericBase`

WITH splash AS (
SELECT DISTINCT SAFE_CAST(_eventMeta.userProperties.userId AS STRING) userId, _eventMeta.processingProperties.tenant tenant, _eventMeta.appProperties.clientType clientType, _eventMeta.appProperties.appVersion appV 
FROM `maximal-furnace-783.askk_analytics.splash_screen_open`
WHERE DATE(time, "Asia/Kolkata") >= "2025-11-30"
AND lower(tenant) like '%askk%'
),

signup as (
select distinct date(time,'Asia/Kolkata') as dt, verifiedUserId as userId, deviceId,
from `maximal-furnace-783.askk_analytics.account_verification`
where date(time,'Asia/Kolkata') between date('2025-11-28','Asia/Kolkata') and CURRENT_DATE('Asia/Kolkata')
and loginType in ('newSignUp','relogin') and status ='success'
and lower(tenant) like '%askk%'
and (lower(clientType) like '%android%' or clientType is null)

union all

select distinct date(time,'Asia/Kolkata') as dt, verifiedUserId as userId, deviceId,
from `maximal-furnace-783.vibely_analytics.account_verification`
where date(time,'Asia/Kolkata') between date('2025-11-28','Asia/Kolkata') and CURRENT_DATE('Asia/Kolkata')
and loginType in ('newSignUp','relogin') and status ='success'
and lower(tenant) like '%askk%'
and (lower(clientType) like '%android%' or clientType is null)
),

ho as (
select distinct date(time,'Asia/Kolkata') as dt, cast(distinct_id as string) userId
from `maximal-furnace-783.askk_analytics.home_opened` a
where date(time,'Asia/Kolkata') >= "2025-11-28"
and lower(tenant) like '%askk%'
and (lower(clientType) like '%android%' or clientType is null)
),

unionUsers AS (

SELECT splash.userId from splash 
UNION DISTINCT 
SELECT ho.userId from ho
UNION DISTINCT
SELECT signup.userId FROM signup
),

user AS (SELECT DISTINCT id userId, name userName, phoneNo from  `maximal-furnace-783.sc_analytics.user`
WHERE tenant = 'askk'),

call10 AS (
SELECT DISTINCT consultee_user_id userId, client_type, MAX(SAFE_CAST(consultation_count AS INTEGER)) as call_no 
FROM `maximal-furnace-783.askk_analytics.consultation`
WHERE DATE(time, "Asia/Kolkata") >= "2025-12-01" AND tenant = 'askk'
GROUP BY ALL
HAVING call_no <=100
)

SELECT DISTINCT 
CURRENT_DATE( "Asia/Kolkata") date, EXTRACT(HOUR FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata") hr, 
unionUsers.userId, user.userName, user.phoneNo, '' state,
-- CASE WHEN (splash.userId IS NOT NULL AND call10.userId IS NULL) THEN 'splash'
-- WHEN (splash.userId IS NOT NULL AND call10.userId IS NOT NULL) THEN 'caller'
-- ELSE NULL END as state,
FROM unionUsers INNER JOIN user On unionUsers.userId = user.userId
ORDER BY 1
;

CREATE OR REPLACE TABLE `maximal-furnace-783.askk_analytics.crmNotificationTemplates` 
AS SELECT * FROM `maximal-furnace-783.askk_analytics.crmNotificationTemplatesSheet`;

