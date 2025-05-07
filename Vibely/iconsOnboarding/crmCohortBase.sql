-- added union all
-- delete from maximal-furnace-783.vibely_analytics.crmCohortBase where date = current_date('Asia/Kolkata');
-- Insert into maximal-furnace-783.vibely_analytics.crmCohortBase
with base as(
    select * from `maximal-furnace-783.vibely_analytics.crmCohortChurn` where date = current_date('Asia/Kolkata')

  union all (
  select * from `maximal-furnace-783.vibely_analytics.crmCohortP2p` where date = current_date('Asia/Kolkata')
  )
  union all (
  select * from `maximal-furnace-783.vibely_analytics.crmCohortF2p` where date = current_date('Asia/Kolkata')
  ) 
  union all (
  select * from `maximal-furnace-783.vibely_analytics.crmVideoOnboardingCohorts` where date = current_date('Asia/Kolkata')
  )
  union all (
  SELECT * FROM `maximal-furnace-783.vibely_analytics.crmRechargeDoneWCPendingWA` WHERE date(date) = current_date("Asia/Kolkata")
  )
  union all (
  SELECT * FROM `maximal-furnace-783.vibely_analytics.IconsOnboardingCohorts` WHERE date(date) = current_date("Asia/Kolkata")  
  )

),

d as (
  select distinct *except(rn) from (
  select * from(
    select *,row_number() over (partition by userId order by rand())rn from base)
  where rn=1
  )
  ),

b2b as (
select distinct userID from (select date, userId, count(date) over (partition by userID)cnt from maximal-furnace-783.vibely_analytics.crmCohortBase
where date between current_date('Asia/Kolkata')-5 and current_date('Asia/Kolkata')-1)
where cnt>=3
),
fin as(select d.* from d left join b2b on d.userid = b2b.userid
where b2b.userid is null),

API as(select date(time,'Asia/Kolkata')date, phoneNo, 
 count(*) over (partition by phoneNo) timesSent,
count(if( data  like '%SUCCESS%',1,null)) over (partition by phoneNo) failTimes FROM `maximal-furnace-783.sc_analytics.respond_api_events`
where date(time,'Asia/Kolkata')>='2024-11-01'
 and workspace='VIBELY' and phoneNo not in ('916387725708','917752846625','918094932667')),
failed as(select distinct phoneNo  from API
 where failTimes/timesSent >0.8 and timesSent>1),
sc as (select distinct phoneNo from maximal-furnace-783.sc_analytics.crmCohortBase where date>=current_date('Asia/Kolkata')-3),
optOut as (select distinct phoneNo from `maximal-furnace-783.vibely_analytics.crmOptedOutUsers`)
 select distinct fin.* from fin left join failed using(phoneNo) left join sc using(phoneNo)
 left join optOut using(phoneNo)
 where optOut.phoneNo is null

 -- left join maximal-furnace-783.Sourabh.tempPriceDrop t using(phoneNo)
 --where failed.phoneNo is null and sc.phoneNo is null --and t.phoneNo is null
