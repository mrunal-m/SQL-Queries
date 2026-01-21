INSERT into `maximal-furnace-783.sc_analytics.FZ_host_comms`
(
SELECT SAFE_CAST(CURRENT_DATE("Asia/Kolkata") as timestamp) date, t1.cohort, 
t1.language, hostId userId, phoneNo, name user_name, template, templateVariables, cdnLinks, 'FZ_USERS' as workSpace, 
current_datetime('Asia/Kolkata') dt_time  
FROM (SELECT *, 'SuperStar_Criteria_Update' cohort FROM (
with usr as (
Select *except(RNum) From
(
SELECT distinct_id AS hostId,chatroomId,language, date(time,'Asia/Kolkata') creation_date,
row_number() over(partition by chatroomID Order By Time) as RNum
FROM `sc-bigquery-product-analyst.data_extraction.make_friends_chatroom_created`
WHERE category IN ('PRIVATE_CONSULTATION','PRIVATE')
)
Where RNum = 1
)
,consultation_global1 as (
select a.*, case when total_charges is null then 0 else total_charges end as total_charges_new, from (
select *, ROW_NUMBER() OVER ( PARTITION BY consultation_id ORDER BY time desc) AS row_num
from (
select * from maximal-furnace-783.sc_analytics.consultation
where date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
and status = 'completed'
)
) a
where row_num = 1
)
,consultation as (
select * from (
select a.*, usr.language,
ROW_NUMBER() OVER ( PARTITION BY consultation_id ORDER BY time desc) AS row_num_1
from consultation_global1 a inner join usr on usr.chatroomId = consultant_id
)
where row_num_1 = 1
)

,consultation_notification_ack as (
select a.*,u.language from (
SELECT time,consultant_id,ping_id,retry_count, row_number() over(partition by consultant_id,ping_id,retry_count order by time desc) as rn
FROM `maximal-furnace-783.sc_analytics.consultation_notification_ack`
where date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
) as a
inner join usr u
on u.chatroomid = a.consultant_id
where rn = 1
)

,active_host_base as (
select a.*,language,hostId from
( select distinct date(time,'Asia/Kolkata') as dt,TIME(time,'Asia/Kolkata') AS tym,consultant_id as chatroomId
from `maximal-furnace-783.sc_analytics.consultant`
where date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
and status = 'ONLINE'
UNION ALL
select distinct date(time,'Asia/Kolkata') as dt, TIME(time,'Asia/Kolkata') AS tym,consultant_id as chatroomId
from consultation
where date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
UNION ALL
select distinct date(time,'Asia/Kolkata') as dt, TIME(time,'Asia/Kolkata') AS tym, consultant_id as chatroomId
from consultation_notification_ack
where retry_count is null
)a inner join usr
on usr.chatroomId = a.chatroomId
group by 1,2,3,4,5
)


,final_status AS (
SELECT *,
CASE WHEN diff = 0 THEN "Active"
WHEN diff = 1 THEN "Inactive Today"
WHEN diff BETWEEN 2 AND 3 THEN "Inactive 3 Days"
WHEN diff BETWEEN 4 AND 7 THEN "Inactive Week"
WHEN diff BETWEEN 8 AND 30 THEN "Inactive for weeks"
ELSE "Inactive Month" END AS status
FROM (
SELECT hostId, last_active_date, CURRENT_DATE('Asia/Kolkata') today,
DATE_DIFF(CURRENT_DATE('Asia/Kolkata'), last_active_date, DAY) diff
FROM (
SELECT hostId, MAX(dt) last_active_date
FROM active_host_base
GROUP BY 1
)
)
)

,banned_hosts as (
select reportedUserId, concat('Banned','-',cast(banType as string)) banType from (
select reportedUserId,banType, from (
SELECT a.actionTaken AS actionTaken,
a.adminName AS adminName,
a.banDurationNew AS banDuration,
b.chatroomId AS chatroomId,
a.chatRoomName AS chatRoomName,
a.language AS language,
a.reportedUserId AS reportedUserId,
date(time,'Asia/Kolkata') as ban_date,
date(time,'Asia/Kolkata') + cast(banDurationNew as INT64) as unban_date,
DATETIME_ADD(datetime(time,'Asia/Kolkata'), INTERVAL cast(banDurationNew as INT64) DAY) AS unban_date_time,
datetime(time,'Asia/Kolkata') as ban_date_time,
a.chatroomType AS chatroomType,
a.banReason AS banReason,
banType,
row_number() over(partition by reportedUserId order by time desc) r1
FROM `maximal-furnace-783.sc_analytics.reported_profile_chatroom_action_taken` a
inner join usr b on reportedUserId=hostid
WHERE date(time,'Asia/Kolkata') >= current_date('Asia/Kolkata')-60
and chatroomType = 'PRIVATE_CONSULTATION_FIND_A_FRIEND'
and actionTaken = 'BANNED'
)
where r1 = 1
and unban_date_time > current_datetime('Asia/Kolkata')

union distinct 

select distinct userId as reportedUserId,'permanentBan' banType
from `maximal-furnace-783.sc_analytics.report_profile_actions` a
join usr b on a.userId = b.hostId
where action in ('PermenantBan','permanentBan')
and DATE(time,'Asia/Kolkata') >= '2024-10-01'
)
)


,current_gold_hosts as (
select *except(r1) from (
SELECT entityId,date(TIMESTAMP_MILLIS(startTime), 'Asia/Kolkata') start_date,category,
row_number() over(partition by entityId order by date(TIMESTAMP_MILLIS(startTime), 'Asia/Kolkata')desc) r1
 FROM `maximal-furnace-783.sc_analytics.store_item_credit`
WHERE date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
and date(TIMESTAMP_MILLIS(endtime), 'Asia/Kolkata') >= current_date('Asia/Kolkata')
and itemType = 'BADGE'
)
where r1 = 1
)


,onboard_type AS (
with referral_manual_hosts as (
SELECT Onboarded_Date date,User_Id as hostId,'Referral_Manual' host_type
FROM `maximal-furnace-783.subodh_fz.referral_hosts_updated` 
where User_Id is not null
group by 1,2
)
,referral_automated_hosts as (
SELECT min(date(time,'Asia/Kolkata')) date,refereeId as hostId,'Referral_Automated' host_type
FROM `maximal-furnace-783.sc_analytics.live_host_referral_events` 
WHERE date(time,'Asia/Kolkata') >= '2025-07-03'
group by 2
)
,in_app_hosts as (
SELECT min(DATE(time,'Asia/Kolkata')) date,user_id hostId,'InApp' host_type
 FROM `maximal-furnace-783.sc_analytics.consultant_onboarding_validation` 
WHERE DATE(time,'Asia/Kolkata') >= "2024-12-13"
group by 2
)
,digital_marketing_hosts as (
SELECT Date date,UID hostId, 'Digital_Marketing' host_type
FROM `maximal-furnace-783.subodh_fz.digital_marketing_leads` 
where UID is not null
group by 1,2
)
,all_leads as (
select *,row_number() over(partition by hostId order by date) r1
from (
select * from referral_manual_hosts
union distinct 
select * from digital_marketing_hosts
)
qualify r1 = 1
)
,in_app_source as (
select distinct_id hostId, referral_source
from (
SELECT distinct_id,referral_source, row_number() over(partition by distinct_id order by time desc) r1
FROM `maximal-furnace-783.sc_analytics.fz_onboarding_entry_point`
WHERE date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
)
where r1 = 1
)

,reviewer as (
select * from (
select user_id,reviewer_email_id,row_number() over(partition by user_id order by time desc) r1 from `maximal-furnace-783.sc_analytics.consultation_creator_access`
)
where r1 = 1
)

select a.hostId,chatroomId,language,creation_date,
case when c.hostId is not null then 'Referral_Automated'
when b.hostId is not null then b.host_type
when d.hostId is not null then 'InApp'
else 'Others' end as host_ob_type,
reviewer_email_id,
from usr a
left join all_leads b on cast(a.hostId as INT64) = b.hostId 
left join referral_automated_hosts c on a.hostId = c.hostId 
left join in_app_hosts d on a.hostId = d.hostId 

left join in_app_source e on a.hostId = e.hostId
left join reviewer f on a.hostId = f.user_id
)


,host_call_details as (
select consultant_user_id hostId, sum(main_GMV_Rs) main_GMV_Rs,
avg(total_active_time) avg_active_time,
sum(total_active_time) total_active_time,
count(distinct dt) active_days
from `maximal-furnace-783.subodh_fz.FZ_HostID_Level_Data`
where dt >= current_date('Asia/Kolkata') - 7
group by 1
)

,delist as (
select * from (
SELECT *, row_number() over(partition by consultant_id order by timestamp desc) r1
FROM `maximal-furnace-783.Saurabh_Live.FZ_delist_whitelistids`
)
where r1 = 1
and Type = 'DELIST'
)

,video_enabled_hosts as (
select userid as hostID from `sc-bigquery-product-analyst.FZ_Video_Supply.fz_video_supply` 
group by 1
)

,icons_hosts as (
select distinct user_id as hostId from (
SELECT *,row_number() over(partition by user_id order by time desc) r1
FROM `maximal-furnace-783.sc_analytics.icons_profile`
WHERE date(time,'Asia/Kolkata') <= current_date('Asia/Kolkata')
and user_id not in ('963287435','709011470','2330786971','1905631352','1953926259','1221753157','1822228168','2911217551','1899094478','1747730464')
)
where r1 = 1
and consultant_type = 'vibely_icon'
)
,vibely_hosts as (
select consultant_user_id as hostId
from `maximal-furnace-783.sc_analytics.consultant` c 
where date(time, 'Asia/Kolkata') <= current_date('Asia/Kolkata')
and consultation_type = 'FIND_A_FRIEND' 
and  consultation_mode IN ('PRIVATE_CONSULTATION','PRIVATE')
and tenant = 'fz'
group by 1
)

select a.*,user_gender,name,phoneNo,
b.last_active_date,b.status,
case 
when c.entityId is not null and c.category = 'FZ_SUPER_HOST_GRADING' then "Gold-SuperStar" 
when c.entityId is not null and c.category = 'FZ_HOST_GRADING' then "Gold-Star" 
else "Non-Gold" end gold_status,
case when d.reportedUserId is not null then banType else "Unbanned" end as ban_status,
CASE WHEN e.agency_hosts is not null THEN "Agency" ELSE "UGC" END host_mapping,
f.host_ob_type,
f.reviewer_email_id,
g.main_GMV_Rs,
g.avg_active_time,
g.active_days,
g.total_active_time,
case when h.consultant_id is not null and i.hostId is not null then 'Both Delisted'
when h.consultant_id is not null and i.hostId is null then 'Both Delisted'
when h.consultant_id is null and i.hostId is null then 'Video Delisted'
when h.consultant_id is null and i.hostId is not null then 'Not delisted'
else 'error' end as delist_status,
case when j.hostId is not null then "Icons" else "Non Icons" end as Icon_status,
case when k.hostId is not null then "Yes" else "No" end as Vibely_host,
from usr a
left join final_status b on a.hostId = b.hostId
left join current_gold_hosts c on a.chatroomId = c.entityId
left join banned_hosts d on a.hostId = d.reportedUserId
LEFT JOIN (SELECT DISTINCT AgencyCreators AS agency_hosts FROM `maximal-furnace-783.subodh_fz.agency_hosts`) e ON CAST(a.hostId AS INT64) = e.agency_hosts
left join onboard_type f on a.hostId = f.hostId
left join host_call_details g on a.hostId = g.hostId
left join delist h on a.chatroomId = h.consultant_id
left join video_enabled_hosts i on a.hostId = i.hostId
left join icons_hosts j on a.hostId = j.hostId
left join vibely_hosts k on a.hostId = k.hostId
left join (select * from `maximal-furnace-783.subodh_fz.user_dump`) l on a.hostId = l.id)
WHERE Vibely_host = 'No' AND last_active_date >= "2026-01-07" AND delist_status = 'Not delisted' AND icon_status = 'Non Icons') t1 LEFT JOIN maximal-furnace-783.sc_analytics.crmTemplatesTemp t 
On t1.cohort = t.cohort AND t1.language = t.language
where template is not null and phoneNo is not null 
AND extract(hour from current_datetime('Asia/Kolkata')) = 15
)
