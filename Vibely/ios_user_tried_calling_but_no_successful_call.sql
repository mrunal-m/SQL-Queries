CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetool_22May_iosCallissue`
AS (
with a as (
with ios_cohort as
     (select consultation_id,
             consultee_user_id as userId,
             max(safe_cast(client_type as string)) as client_type
      from `maximal-furnace-783.sc_analytics.consultation`
      where date(time, 'Asia/Kolkata') = current_date('Asia/Kolkata')
        and consultation_type = 'FIND_A_FRIEND'
        and tenant = "fz"
      group by 1,
               2
      having lower(client_type) like '%ios%'),
        consultation AS
     (SELECT *,
             CASE
                 WHEN (discounted_fee_per_minute = 5
                       AND discounted_max_minutes = 5
                       AND total_charges>25) THEN 5 + (total_charges - 25)/3.5
                 WHEN (discounted_fee_per_minute = 5
                       AND discounted_max_minutes = 5
                       AND total_charges<=25) THEN total_charges/5.0
                 WHEN (discounted_fee_per_minute = 1
                       AND discounted_max_minutes = 5
                       AND total_charges>5) THEN 1 + (total_charges - 5)/3.5
                 WHEN (discounted_fee_per_minute = 1
                       AND discounted_max_minutes = 5
                       AND total_charges<=5) THEN total_charges/5.0
                 WHEN (discounted_fee_per_minute=0
                       AND discounted_max_minutes = 6
                       AND total_charges>0) THEN 0 + (total_charges)/3.5
                 WHEN (discounted_fee_per_minute=0
                       AND discounted_max_minutes = 6
                       AND total_charges=0) THEN 0
                 ELSE total_charges/3.5
             END AS gmv_inr,
             CASE
                 WHEN ((discounted_fee_per_minute = 5
                        AND discounted_max_minutes = 5
                        AND total_charges <= 25)
                       OR (discounted_fee_per_minute = 0
                           AND discounted_max_minutes = 6
                           AND total_charges = 0)
                       OR (discounted_fee_per_minute = 1
                           AND discounted_max_minutes = 5
                           AND total_charges <= 5)) THEN consultation_id
                 ELSE NULL
             END AS wc_free,
             CASE
                 WHEN ((discounted_fee_per_minute = 5
                        AND discounted_max_minutes = 5
                        AND total_charges > 25)
                       OR (discounted_fee_per_minute = 0
                           AND discounted_max_minutes = 6
                           AND total_charges > 0)
                       OR (discounted_fee_per_minute = 1
                           AND discounted_max_minutes = 5
                           AND total_charges > 5)) THEN consultation_id
                 ELSE NULL
             END AS wc_extended,
             CASE
                 WHEN ((discounted_fee_per_minute = 5
                        AND discounted_max_minutes = 5)
                       OR (discounted_fee_per_minute = 0
                           AND discounted_max_minutes = 6)
                       OR (discounted_fee_per_minute = 1
                           AND discounted_max_minutes = 5)) THEN consultation_id
                 ELSE NULL
             END AS wc
      FROM
        (SELECT a.*,
                TIMESTAMP_DIFF(TIMESTAMP(session_ended_at), TIMESTAMP(session_started_at), SECOND) / 60.0 AS total_call_time,
                DATE(time, 'Asia/Kolkata') AS dt,
                ROW_NUMBER() OVER (PARTITION BY a.consultation_id,
                                                vendor_session_id,
                                                status
                                   ORDER BY time) AS rn
         FROM `maximal-furnace-783.sc_analytics.consultation` a
         join ios_cohort b on a.consultation_id =b.consultation_id
         WHERE DATE(time, 'Asia/Kolkata') = CURRENT_DATE("Asia/Kolkata")
           and EXTRACT(HOUR from time AT TIME ZONE "Asia/Kolkata") BETWEEN 15 AND 17
           AND consultation_type = 'FIND_A_FRIEND'
           AND tenant = 'fz' )
      WHERE rn = 1
        --AND status = 'completed' 
        ),

connecting as(
  select distinct consultee_user_id, 
  from consultation
  where status = 'connecting'
),

completed as(
  select distinct consultee_user_id
  from consultation
  where status = 'completed'
)

select distinct a.consultee_user_id as userId, 'English' language, 'Friend' userName, '123456789' phoneNo
from connecting a left join completed b 
on a.consultee_user_id = b.consultee_user_id
where b.consultee_user_id is null

),

b as (select * from  `maximal-furnace-783.vibely_analytics.crmNotificationsWithTemplateFinal`
limit 1 ),
f as(
select date, 
extract(hour from current_time('Asia/Kolkata')) hour,
a.userId,
a.userName,
a.phoneNo,
callsLifetime,
notificationsSent,
'feed' as target,
'adHoc' cohort,
'22May_iosCallissue' as templateId,
"Sorry, you tried to call but couldn’t!😞" title, 
'We’re back online🚀 Call your friends now!'text, 
 'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/1fb57083_1729256150911_sc.png' thumbnailUrl, 
notifRank, 
setRank,
setMap,
a.language,
concat('{"userName":','"', a.userName,'"', '}') as templateVariables,
rowNumber,row_number() over(partition by a.userId order by rand())rn 
from b left join a on true
--left join `maximal-furnace-783.vibely_analytics.crmNotificationTemplates` t on t.state='offer' and t.templateId like '%tier_drop%'
qualify rn=1)
select *except(rowNumber,rn),row_number() over ()rowNumber from f
)
