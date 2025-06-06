delete from  maximal-furnace-783.vibely_analytics.crmNotificationsRealtimeWithTemplate
where date(date)=current_date('Asia/Kolkata') and hour =extract(hour from current_datetime('Asia/Kolkata'));
insert into maximal-furnace-783.vibely_analytics.crmNotificationsRealtimeWithTemplate
with a as(with 
copyCycle as (
  select * from maximal-furnace-783.vibely_analytics.crmNotificationsCopyCycle 
  union distinct
      (with a as(SELECT distinct text,
    distinct_id userID,
    SPLIT(communityNotifId, '/')[4] id
    FROM `maximal-furnace-783.vibely_analytics.notification_initiated`
    Where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') and type like '%fz%'),
    b as(
      select distinct 
    SPLIT(communityNotifId, '/')[3] copy,
    SPLIT(communityNotifId, '/')[4] id,
    distinct_id userID
    FROM `maximal-furnace-783.vibely_analytics.notification_issued`
    Where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') and type like '%fz%'
    )

    select distinct text,copy,a.userId from a join b using(id))
),
context as (select distinct * from maximal-furnace-783.vibely_analytics.crmNotificationsContextualAudioNotifs 
where date=(select max(date) from maximal-furnace-783.vibely_analytics.crmNotificationsContextualAudioNotifs  )),
user as (
  select distinct userid, language, phoneNo,name from (
    select id userId, language,phoneNo,name,row_number() over (partition by id order by createdOn desc)rn from `maximal-furnace-783.sc_analytics.user`
    where length(phoneNo)<=14 and tenant ='fz'
    )
  where rn = 1
),
hosts as (select userId, any_value(name)name from `maximal-furnace-783.vibely_analytics.crmNotificationsHostNames` group by 1),

mHour as (select max(hour) mh from maximal-furnace-783.vibely_analytics.hostAffinityPairs where date=current_date('Asia/Kolkata')),

base as(select * from (select h.*except( _3DaysGMV,
 _7DaysGMV,
 _30DaysGMV,userName,
  _3DaysVideoGMV,
 _7DaysVideoGMV,
 _30DaysVideoGMV
 ),coalesce(u1.name,'friend') as userName, coalesce(u2.name,'your friend') as hostName,'bottomSheet' target, 
 if(c.title is not null,'contextual',templateId)templateId,
 if(c.translated_title is not null,c.translated_title,t.title)title,
 if(c.translated_text is not null,c.translated_text,t.text)text,
 if(c.title is not null,'https://cdn-sc-g.sharechat.com/33d5318_1c8/live/631c416_1735923606410_sc.png', thumbnailUrl)thumbnailUrl, 
 if(c.title is not null,null,templateVariables)templateVariables, row_number() over (partition by h.userId, h.hostId order by rand())rn  from maximal-furnace-783.vibely_analytics.hostAffinityPairs h 

left join user u1 on h.userid = u1.userId
left join maximal-furnace-783.vibely_analytics.crmNotificationTemplates t 
on target ='bottomSheetRealtime' and (t.language = u1.language or t.language='English')
--and (if(current_date('Asia/Kolkata') > '2024-12-25' and templateId not like '%christmas%',true,templateId like '%christmas%'))
left join hosts u2 on h.hostUserid = u2.userId
left join context c on h.hostUserid = c.hostId and c.userid = h.userID
left join copyCycle cy on c.userId = cast(cy.userid as string) and c.translated_text = cy.text
--left join copyCycle cy2 on h.userId = cast(cy2.userid as string) and t.templateId = cy2.copy
where h.date = current_date('Asia/Kolkata') and h.hour = (select * from mHour) and cy.text is null --and cy2.copy is not null
)
where rn=1)

select cast(date as timeStamp)date, * except(date,templateVariables,type,templateId,affinity),type as affinityCohort,templateId as template,  case
when templateVariables='userName' and userName is not null then concat('{"userName":','"', userName,'"', '}')
when templateVariables = "hostName" and hostName is not null then concat('{"hostName":','"', hostName,'"', '}')
when templateVariables = "userName, hostName"and userName is not null  and hostName is not null then  concat('{"userName":','"', userName,'"',',','"hostName":','"', hostName,'"','}')
else null
end as templateVariables, null batchSize, null delay,'AUDIO' as affinity,if(affinity='VIDEO','both','single')affinityType from base
where userID is not null and hostId is not null),

b as(

  with 
copyCycle as (
  select * from maximal-furnace-783.vibely_analytics.crmNotificationsCopyCycle 
  union distinct
      (with a as(SELECT distinct text,
    distinct_id userID,
    SPLIT(communityNotifId, '/')[4] id
    FROM `maximal-furnace-783.vibely_analytics.notification_initiated`
    Where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') and type like '%fz%'),
    b as(
      select distinct 
    SPLIT(communityNotifId, '/')[3] copy,
    SPLIT(communityNotifId, '/')[4] id,
    distinct_id userID
    FROM `maximal-furnace-783.vibely_analytics.notification_issued`
    Where date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata') and type like '%fz%'
    )

    select distinct text,copy,a.userId from a join b using(id))
),
context as (select distinct * from maximal-furnace-783.vibely_analytics.crmNotificationsContextualAudioNotifs 
where date=(select max(date) from maximal-furnace-783.vibely_analytics.crmNotificationsContextualAudioNotifs  )),
user as (
  select distinct userid, language, phoneNo,name from (
    select id userId, language,phoneNo,name,row_number() over (partition by id order by createdOn desc)rn from `maximal-furnace-783.sc_analytics.user`
    where length(phoneNo)<=14 and tenant ='fz'
    )
  where rn = 1
),
hosts as (select userId, any_value(name)name from `maximal-furnace-783.vibely_analytics.crmNotificationsHostNames` group by 1),

mHour as (select max(hour) mh from maximal-furnace-783.vibely_analytics.hostAffinityPairs where date=current_date('Asia/Kolkata')),


base as(select * from (select h.*except( _3DaysGMV,
 _7DaysGMV,
 _30DaysGMV,userName,
  _3DaysVideoGMV,
 _7DaysVideoGMV,
 _30DaysVideoGMV
 ),coalesce(u1.name,'friend') as userName, coalesce(u2.name,'your friend') as hostName,'bottomSheet' target, 
 if(c.title is not null,'contextual',templateId)templateId,
 if(c.translated_title is not null,c.translated_title,t.title)title,
 if(c.translated_text is not null,c.translated_text,t.text)text, 
 if(c.title is not null,'https://cdn-sc-g.sharechat.com/33d5318_1c8/live/631c416_1735923606410_sc.png', thumbnailUrl)thumbnailUrl, 
 if(c.title is not null,null,templateVariables)templateVariables, row_number() over (partition by h.userId, h.hostId order by rand())rn  from maximal-furnace-783.vibely_analytics.hostAffinityPairs h 

left join user u1 on h.userid = u1.userId
left join maximal-furnace-783.vibely_analytics.crmNotificationTemplates t 
on target ='bottomSheetRealtimeVideo' and (t.language = u1.language or t.language='English')
--and (if(current_date('Asia/Kolkata') > '2024-12-25' and templateId not like '%christmas%',true,templateId like '%christmas%'))
left join hosts u2 on h.hostUserid = u2.userId
left join context c on h.hostUserid = c.hostId and c.userid = h.userID
left join copyCycle cy on c.userId = cast(cy.userid as string) and c.translated_text = cy.text
--left join copyCycle cy2 on h.userId = cast(cy2.userid as string) and t.templateId = cy2.copy
where h.date = current_date('Asia/Kolkata') and h.hour = (select * from mHour) and cy.text is null --and cy2.copy is not null
and h.affinity='VIDEO'
)
where rn=1)

select cast(date as timeStamp)date, * except(date,templateVariables,type,templateId,affinity),type as affinityCohort,templateId as template,  case
when templateVariables='userName' and userName is not null then concat('{"userName":','"', userName,'"', '}')
when templateVariables = "hostName" and hostName is not null then concat('{"hostName":','"', hostName,'"', '}')
when templateVariables = "userName, hostName"and userName is not null  and hostName is not null then  concat('{"userName":','"', userName,'"',',','"hostName":','"', hostName,'"','}')
else null
end as templateVariables, 120 batchSize, GREATEST(SAFE_CAST(base.avgPCT AS INT)-2, 2) as delay,'VIDEO' as affinity,'both' affinityType from base
where userID is not null and hostId is not null
)
select * from a
union distinct
select * from b
