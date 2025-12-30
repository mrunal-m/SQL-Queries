CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmFlashSaleNotificationsExpAllocation`
AS SELECT userId, 
CASE WHEN MOD(ABS(FARM_FINGERPRINT(userId)), 2) = 0 THEN 'control' ELSE 'variant-1' END AS variant 
FROM 
(SELECT DISTINCT SAFE_CAST(a.distinct_id AS STRING) userId,
FROM `maximal-furnace-783.vibely_analytics.home_opened` a
INNER JOIN `maximal-furnace-783.sc_analytics.user`b ON
a.distinct_id = SAFE_CAST(b.id AS INT64)
WHERE date(a.time, "Asia/Kolkata") >= "2025-10-01" AND b.tenant = 'fz'
AND LOWER(a.clientType) = 'android'
--AND b.language IN ('Malayalam', 'Telugu', 'Hindi', 'Gujarati')
);
