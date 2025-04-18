-- CREATE OR REPLACE TABLE `maximal-furnace-783.sc_analytics.crmHostNotificationsTemplates`
-- AS 
-- SELECT * FROM `maximal-furnace-783.sc_analytics.crmHostNotificationSheet`

--task1 host earnings & notif templates
CREATE OR REPLACE TABLE `maximal-furnace-783.sc_analytics.crmHostNotificationsHostEarnings`
AS
SELECT consultant_user_id hostId, AVG(calls) avg_calls_per_day, AVG(host_earnings) avg_host_earnings, 
APPROX_QUANTILES(host_earnings, 100)[OFFSET(50)] AS p50_earnings
FROM 
(SELECT date(time)dt, consultant_user_id, count(consultation_id) calls, 
(SUM(CASE WHEN tenant IS NULL OR tenant = 'sc' THEN total_charges / 4.8  
 WHEN tenant = 'fz' THEN total_charges / 3.5  END)*0.30) AS host_earnings,    
 FROM 
 ( SELECT *,  ROW_NUMBER() OVER (PARTITION BY consultation_id ORDER BY time DESC) AS row_num 
      FROM `maximal-furnace-783.sc_analytics.consultation`
      WHERE DATE(time, 'Asia/Kolkata') <= CURRENT_DATE('Asia/Kolkata') -- Include consultations up to today
        AND status = 'completed'                                       -- Only completed consultations
        AND consultation_type = 'FIND_A_FRIEND'    
   ) WHERE row_num=1
 GROUP BY ALL)
GROUP BY ALL
ORDER BY 4 DESC;


SELECT * from `maximal-furnace-783.sc_analytics.consultation`
      WHERE DATE(time, 'Asia/Kolkata') <= CURRENT_DATE('Asia/Kolkata') -- Include consultations up to today
        AND status = 'completed'                                       -- Only completed consultations
        AND consultation_type = 'FIND_A_FRIEND'    
  AND consultant_user_id IN ('3427164755','1935256133',  '2372964619')
