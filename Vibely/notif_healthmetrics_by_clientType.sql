With t1 AS (SELECT date(time, "Asia/Kolkata") dt, LOWER(clientType) clientType, COUNT(DISTINCT distinct_id) count
FROM  `maximal-furnace-783.vibely_analytics.notification_initiated` 
WHERE (status is null or status = 'init') AND date(time,"Asia/Kolkata") >= current_date() - 15
GROUP BY ALL ),
t2 AS (SELECT date(time, "Asia/Kolkata") dt, LOWER(clientType) clientType, COUNT(DISTINCT distinct_id) count 
FROM `maximal-furnace-783.vibely_analytics.notification_issued` WHERE date(time,"Asia/Kolkata") >= current_date() - 15
GROUP BY ALL ),
t3 AS (SELECT date(time, "Asia/Kolkata") dt, LOWER(clientType) clientType, COUNT(DISTINCT distinct_id) count 
FROM `maximal-furnace-783.vibely_analytics.notification_clicked` WHERE date(time,"Asia/Kolkata") >= current_date() - 15
GROUP BY ALL )

SELECT t1.dt, t1.clientType, t1.count initiated, t2.count issued, t3.count clicked from t1 
LEFT JOIN t2 ON t1.dt = t2.dt AND t1.clientType = t2.clientType
LEFT JOIN t3 ON t2.dt = t3.dt AND t2.clientType = t3.clientType
ORDER BY 1, 2
