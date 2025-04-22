CREATE OR REPLACE TABLE `maximal-furnace-783.Sourabh.HostCRMAnalyticsTemp`
AS
WITH t1 AS (SELECT date(time, "Asia/Kolkata") date, SPLIT(communityNotifId, '/')[1] cohort, distinct_id, notifId, --title, text  
from `maximal-furnace-783.sc_analytics.notification_initiated`
where DATE(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 5  and type = "sc_fz_host_engagement"  
AND status = 'init'
GROUP BY ALL),

t2 AS (SELECT date(time, "Asia/Kolkata") date, SPLIT(communityNotifId, '/')[1] cohort,distinct_id,  notifId  
from `maximal-furnace-783.sc_analytics.notification_issued`
where DATE(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 5  and type = "sc_fz_host_engagement"  
GROUP BY ALL ),

t3 AS (SELECT date(time, "Asia/Kolkata") date, SPLIT(communityNotifId, '/')[1] cohort, distinct_id,  notificationId  notifId
from `maximal-furnace-783.sc_analytics.notification_clicked`
where DATE(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 5  and type = "sc_fz_host_engagement"  
GROUP BY ALL )

SELECT t1.date, t1.cohort, 
COUNT(distinct t1.distinct_id) hosts_initiated, COUNT(distinct t2.distinct_id) hosts_delivered, COUNT(distinct t3.distinct_id) hosts_clicked, 
COUNT(distinct t1.notifId) notif_initiated, COUNT(distinct t2.notifId) notif_delivered, COUNT(distinct t3.notifId) notif_clicked, 
FROM t1 LEFT JOIN t2 ON t1.date = t2.date AND t1.cohort = t2.cohort
LEFT JOIN t3 ON t2.date = t3.date AND t2.cohort = t3.cohort
GROUP BY ALL
ORDER BY 1, 2
