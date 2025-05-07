-- added 2 new cohorts
-- delete from `maximal-furnace-783.vibely_analytics.crmDataWithTemplate` where date(date)= current_date('Asia/Kolkata');
-- insert into `maximal-furnace-783.vibely_analytics.crmDataWithTemplate` 

(
select date, 
 CASE 
    WHEN COUNT(CASE WHEN cohort = 'video' THEN 1 END) > 0 THEN 'videoCall'
    ELSE MIN(cohort) END as cohort, language, UserId, phoneNo, user_name, template, templateVariables, cdnUrl
from (

with base as (select *, 
case 
when cohort = 'VideoAppUpdate' then 'VideoAppUpdate'
when cohort = 'DoVideoCall' then 'DoVideoCall'
when cohort ='p2p' then 'Retention' 
when cohort in ('churn','churn5+','pctDrop') then 'Churn'
when cohort ='f2p' then 'Activation'
when cohort = '1st Recharge' then '1st Recharge'
when cohort = 'IconsOnboarding' then 'IconsOnboarding'
when cohort = 'IconsAppUpdate' then 'IconsAppUpdate'
when cohort = 'freeCallMiss' then 'Free Call Miss' end as cohortFinal from maximal-furnace-783.vibely_analytics.crmCohortBase
where date= current_date('Asia/Kolkata'))

select distinct cast(date as timestamp) date,*except(rn,date)from (
  select base.date,cohortFinal as cohort, base.*except(date,cohort,cohortFinal), template,
  case when templateVariables='userName' then 
  concat('{"body" : ["' ,user_name,'"],"header" : ["',cdnLinks,'"]}') else
  concat('{"header" : ["',cdnLinks,'"]}')   end as templateVariables
  , row_number() over (partition by UserId order by rand())rn,cdnLinks as cdnUrl from base 
  left join `maximal-furnace-783.vibely_analytics.crmTemplatesTemp` t 
  on --base.language = t.language and 
  base.cohortFinal = t.Cohort
  where template is not null and phoneNo is not null
  )
where rn = 1 and phoneNo is not null and template is not null  
AND userId NOT IN (SELECT userId from sc-bigquery-product-analyst.parikshith.ReactivationComms_users WHERE tenant = 'fz')
)
GROUP BY ALL)
