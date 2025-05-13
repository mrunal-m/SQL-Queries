-- delete from maximal-furnace-783.vibely_analytics.hostAffinityPairs
-- where date=current_date('Asia/Kolkata') and hour = extract(hour from current_datetime('Asia/Kolkata'));
-- insert into `maximal-furnace-783.vibely_analytics.hostAffinityPairs`

-- rnk, batchsize, delay

with usr AS ( SELECT distinct consultant_id chatroomId,consultation_language language
FROM `maximal-furnace-783.sc_analytics.consultant`
WHERE consultation_mode IN ('PRIVATE_CONSULTATION', 'PRIVATE')
and date(time, "Asia/Kolkata") >= current_date() - 30 
and (consultation_type ="FIND_A_FRIEND" or consultation_type is null)),

consultation_global as (SELECT a.*, case when total_charges is null then 0 else total_charges end as total_charges_new from 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY consultation_id ORDER BY time desc) AS row_num
FROM (SELECT * from maximal-furnace-783.sc_analytics.consultation
WHERE DATE(time,'Asia/Kolkata') between current_date('Asia/Kolkata')-30 and current_date('Asia/Kolkata')
and status = 'completed'
and consultation_type = 'FIND_A_FRIEND' and tenant='fz')) a
WHERE row_num = 1),


consultation as (SELECT *, TIMESTAMP_DIFF(CAST(session_ended_at AS TIMESTAMP), CAST(session_started_at AS TIMESTAMP), SECOND) AS conversationDuration from (SELECT a.*, usr.language, ROW_NUMBER() OVER (PARTITION BY consultation_id ORDER BY time desc) AS row_num_1
from consultation_global a inner join usr on usr.chatroomId = consultant_id)
WHERE row_num_1 = 1),

lastType as (SELECT distinct consultee_user_id userId,consultant_id hostId,media_type, 
ROW_NUMBER() OVER (partition by consultee_user_id,consultant_id order by time desc)lRn from consultation
WHERE status = 'completed' and total_charges_new > 0
qualify lRn=1),

pairs as (SELECT DISTINCT DATE(time,'Asia/Kolkata') date, consultee_user_id as userID, language, consultation_id, consultant_id hostID,
consultant_user_id hostUserId, total_charges_new, media_type, SUM(CAST(ConversationDuration/60 AS FLOAT64)) AS paid_call_time,
FROM consultation
WHERE status = 'completed' AND total_charges_new > 0
group by all ),

pct as (SELECT hostID, sum(paid_call_time)/count(distinct consultation_id) avgPCT from pairs
  where date>=current_date('Asia/Kolkata')-7
  group by 1),

gmv as (SELECT *, ROW_NUMBER() OVER (partition by date, userid order by _3DayRank+_7DayRank+_30DayRank) rnk from 
  (SELECT *, 
  row_number() over (partition by date,userid order by _3DaysGMV desc) _3DayRank ,
  row_number() over (partition by date,userid order by _7DaysGMV desc) _7DayRank ,
  row_number() over (partition by date,userid order by _30DaysGMV desc) _30DayRank 
  from (
  select current_date('Asia/Kolkata')date, userId, hostId, hostUserId,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=3,GMV, 0))  _3DaysGMV,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=7,GMV, 0))  _7DaysGMV,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=30,GMV, 0))  _30DaysGMV,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=30,_3minCalls, 0))  _303minCalls,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=30,Calls, 0))  _30DaysCalls,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=3,video_GMV, 0))  _3DaysVideoGMV,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=7,video_GMV, 0))  _7DaysVideoGMV,
  sum(if(date_diff(current_date('Asia/Kolkata'),date,day)<=30,video_GMV, 0))  _30DaysVideoGMV
  
from (select date, userId, hostId, any_value(hostUserId)hostUserId, sum(total_charges_new) GMV, count(distinct consultation_id) calls,
  count(distinct if(paid_call_time>3,1,null)) _3minCalls,
  ifnull(sum(case when media_type = "VIDEO" then total_charges_new else 0 end),0) as video_GMV
   from pairs group by 1,2,3
having calls>=1 or _3minCalls>=1)
  group by 1,2,3,4)
  
)),

user as (select distinct id,any_value(name)name from `maximal-furnace-783.vibely_analytics.user`
where name is not null group by 1)

select *except( _30DaysCalls,_303minCalls) from (select distinct current_date('Asia/Kolkata')date, 
extract(hour from current_datetime('Asia/Kolkata'))hour, 
case when (_7DaysGMV>=100) then 'GMV'
when _303minCalls>=1 then '3minCall' 
when _30DaysCalls>=1 then 'call' end as type,
 gmv.userId, gmv.hostId,hostUserId,rnk, avgPCT,
 _3DaysGMV,
 _7DaysGMV,
 _30DaysGMV,
 name userName,
 _3DaysVideoGMV,
 _7DaysVideoGMV,
 _30DaysVideoGMV,
 case 
 when _3DaysGMV!=0 and _3DaysVideoGMV>= 0.40*(_3DaysGMV) then 'VIDEO' 
 when _7DaysGMV!=0 and _7DaysVideoGMV>=0.30*(_7DaysGMV) then 'VIDEO' 
 when _30DaysGMV!=0 and _30DaysVideoGMV>=0.30*(_30DaysGMV) then 'VIDEO' 
 else l.media_type  end as affinity,
 _30DaysCalls,
 _303minCalls

  from gmv left join pct on gmv.hostId = pct.hostID
 left join user u on gmv.userId = u.id
 left join lastType l on gmv.userId = l.userId and gmv.hostID = l.hostID
)
where  ((rnk<=5 and _30DaysCalls>=1) or (_7DaysGMV>=100 and rnk<=10) or (_303minCalls>=1 and rnk<=8) ) or (LOWER(affinity)='video' and rnk<=30)
order by date, userid, rnk
