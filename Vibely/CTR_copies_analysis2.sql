create temp function start_date() as (DATE_SUB(current_date('Asia/Kolkata'), INTERVAL 3 DAY));
create temp function end_date() as (DATE_SUB(current_date('Asia/Kolkata'), INTERVAL 1 DAY));


-- delete from `maximal-furnace-783.vibely_analytics.crmTemplateAnalytics`
-- where date between start_date() and end_date();
-- insert into maximal-furnace-783.vibely_analytics.crmTemplateAnalytics 


with a as(
  select distinct date(time,'Asia/Kolkata')date,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification%" then 'gamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  SPLIT(communityNotifId, '/')[3] templateId, title, text,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID from `maximal-furnace-783.vibely_analytics.notification_initiated`
  where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%' and (status is null or status = 'init')
),

b as(
  SELECT distinct date(time,'Asia/Kolkata')date,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification%" then 'gamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] as target,
  SPLIT(communityNotifId, '/')[3] templateId, 
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  FROM `maximal-furnace-783.vibely_analytics.notification_issued`
  Where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%'
),

c as(
  select distinct  date(time,'Asia/Kolkata')date, 
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification%" then 'gamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  SPLIT(communityNotifId, '/')[3] templateId,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID
  from `maximal-furnace-783.vibely_analytics.notification_clicked`
  where date(time,'Asia/Kolkata') between start_date() and end_date() and type like '%fz%'
),

f as( select * from `maximal-furnace-783.vibely_analytics.crmNotificationTemplates`),

base as(select distinct a.title, a.text, a.date, if(a.state like '%call%','call',a.state) state, a.templateId,  
count(distinct a.id)initiated, count(distinct b.id) issued, count(distinct c.id) clicked,  from a
left join b on a.date= b.date and a.id = b.id and a.templateId = b.templateId
left join c on a.date= c.date and a.id = c.id and a.templateId = c.templateId
group by ALL
order by 1, 2),
base2 AS (
select distinct f.title, f.text, base.*except(title, text),  from base INNER join f on base.templateId= f.templateId
UNION ALL
SELECT base.* FROM base WHERE LOWER(templateId) LIKE "%adhoc%")

SELECT * from base2

