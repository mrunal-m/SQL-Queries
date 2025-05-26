SELECT state, communityNotifId from (SELECT *, ROW_NUMBER() OVER(PARTITION BY state ORDER BY RAND()) rn FROM (select distinct date(time,'Asia/Kolkata')date, communityNotifId,
  case when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='VIDEO' then 'realtimeVideo'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime' and SPLIT(communityNotifId, '/')[4] ='AUDIO' then 'realtime'
  when SPLIT(communityNotifId, '/')[0]='fz_realtime'  then 'realtime'
  WHEN LOWER(communityNotifId) LIKE "%fz_user_reactivation%" then 'reactivation'
  WHEN LOWER(communityNotifId) LIKE "%fz_gamification%" then 'gamification'
  else SPLIT(communityNotifId, '/')[1] end as state,
  SPLIT(communityNotifId, '/')[2] target,
  SPLIT(communityNotifId, '/')[3] templatId, title, text,
  if(length(SPLIT(communityNotifId, '/')[4])<36, SPLIT(communityNotifId, '/')[6],SPLIT(communityNotifId, '/')[4]) id,
  distinct_id userID from `maximal-furnace-783.vibely_analytics.notification_initiated`
  where date(time,'Asia/Kolkata') >= current_date()-2))
  WHERE rn IN (1)
