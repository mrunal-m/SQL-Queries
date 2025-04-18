INSERT INTO  `maximal-furnace-783.sc_analytics.crmHostNotificationsUnbanned` 

SELECT DISTINCT current_date("Asia/Kolkata") date, EXTRACT(HOUR from current_timestamp() AT TIME ZONE "Asia/Kolkata") as hour, 
"HostUnbanned" cohort, userId,  userName, language, phoneNo
FROM (
WITH usr AS (
  SELECT DISTINCT DISTINCT_ID AS hostId, host_name, chatroomId, language, time AS creation_time
  FROM `sc-bigquery-product-analyst.data_extraction.make_friends_chatroom_created`
  WHERE category IN ('PRIVATE_CONSULTATION', 'PRIVATE')),
  t2 AS (
SELECT DISTINCT 
  a.reportedUserId userId, 
  DATETIME(TIMESTAMP(a.time), 'Asia/Kolkata') AS unban_datetime_ist
FROM `maximal-furnace-783.sc_analytics.reported_profile_chatroom_action_taken` a
--CROSS JOIN time_bounds t
WHERE a.chatroomType = 'PRIVATE_CONSULTATION_FIND_A_FRIEND' and DATE(a.time, 'Asia/Kolkata') >= CURRENT_DATE('Asia/Kolkata')-60
  AND a.actionTaken = 'UNBANNED'
  AND DATETIME(TIMESTAMP(a.time), 'Asia/Kolkata') BETWEEN DATETIME(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AND DATETIME(CURRENT_TIMESTAMP())
  )
  SELECT t2.userId, t2.unban_datetime_ist, usr.host_name userName, usr.language, t3.phoneNo

  from t2 LEFT JOIN usr
  ON t2.userId = usr.hostId
  LEFT JOIN `maximal-furnace-783.sc_analytics.user` t3 ON usr.hostId = t3.id
)
