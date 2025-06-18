-- WARNING!!! date should be in timestamp() type but without UTC part
-- CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_1stRecharge_18Jun`
--   LIKE `maximal-furnace-783.vibely_analytics.crmDataWithTemplate`;

insert into `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_1stRecharge_18Jun` 
(
select date, 
 CASE 
    WHEN COUNT(CASE WHEN cohort = 'video' THEN 1 END) > 0 THEN 'videoCall'
    ELSE MIN(cohort) END as cohort, language, UserId, phoneNo, userName, template, templateVariables, cdnUrl
from (

with base as ( (
WITH recharge_success as (
SELECT DISTINCT date(time, "Asia/Kolkata") recharge_date, userId
FROM maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event recharge
WHERE cost IN (1, 9, 19)  AND status = "SUCCESS"
AND date(time, "Asia/Kolkata") BETWEEN "2025-06-10" AND "2025-06-16"
),


callers as ( SELECT DISTINCT * FROM (
SELECT distinct isWelcomeCallCompleted userId from maximal-furnace-783.vibely_analytics.installerDailyUserAgg
WHERE isWelcomeCallCompleted IS NOT NULL
UNION ALL
SELECT DISTINCT consultee_user_id AS userId,  
    FROM `maximal-furnace-783.sc_analytics.consultation` consultation
    WHERE date(time, "Asia/Kolkata") BETWEEN "2025-06-10" AND "2025-06-16"
    AND consultation_type = 'FIND_A_FRIEND'
    AND tenant = "fz" 
    AND status = 'completed'
)
),

not_called_users as (
SELECT DISTINCT recharge_date,  t1.userId
FROM recharge_success t1 LEFT JOIN callers ON t1.userId = callers.userId
WHERE callers.userId IS NULL),

t2 AS (SELECT Id userId, name userName, phoneNo, language from `maximal-furnace-783.sc_analytics.user` WHERE tenant = 'fz'
AND LENGTH(phoneNo)<=14)


SELECT DISTINCT current_date("Asia/Kolkata") as date, "1st Recharge" as cohortFinal, "English" as language, not_called_users.userId, t2.phoneNo, t2.userName
from not_called_users INNER JOIN t2 ON not_called_users.userId = t2.userId))

select distinct cast(date as timestamp) date,*except(date)from (
  select base.date, cohortFinal as cohort, base.*except(date,cohortFinal), template,
  case when templateVariables='userName' then 
  concat('{"body" : ["' ,userName,'"],"header" : ["',cdnLinks,'"]}') else
  concat('{"header" : ["',cdnLinks,'"]}')   end as templateVariables
  , row_number() over (partition by UserId order by rand())rn,cdnLinks as cdnUrl from base 
  left join `maximal-furnace-783.vibely_analytics.crmTemplatesTemp` t 
  on --base.language = t.language and 
  base.cohortFinal = t.Cohort
  where template is not null and phoneNo is not null
  )
where rn = 1 and phoneNo is not null and template LIKE "%recharge%"  
)
GROUP BY ALL)
