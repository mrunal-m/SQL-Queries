--Fetching Recommendations from BQ Table

(SELECT *, 
CASE WHEN (resource LIKE "%3572872517%") THEN "moj-prod"
WHEN (resource LIKE "%735797477699%") THEN "maximal-furnace-783"
WHEN (resource LIKE "%372256583614%") THEN "sharechat-production"
WHEN (resource LIKE "%760691384490%") THEN "sc-bigquery-product-analyst"
WHEN (resource LIKE "%412826138735%") THEN "sharechat-firebase"
ELSE resource END as project_name
from maximal-furnace-783.data_platform_temp1.partition_cluster_recommender
WHERE date(recommendationGenerationTimestamp)= "2022-11-16"
ORDER BY slotMsSavedMonthly)
UNION ALL
(SELECT *, 
CASE WHEN (resource LIKE "%3572872517%") THEN "moj-prod"
WHEN (resource LIKE "%735797477699%") THEN "maximal-furnace-783"
WHEN (resource LIKE "%372256583614%") THEN "sharechat-production"
WHEN (resource LIKE "%760691384490%") THEN "sc-bigquery-product-analyst"
WHEN (resource LIKE "%412826138735%") THEN "sharechat-firebase"
ELSE resource END as project_name
from maximal-furnace-783.data_platform_temp1.partition_cluster_recommender
WHERE date(recommendationGenerationTimestamp)= "2022-11-07" AND resource LIKE "%760691384490%"
ORDER BY slotMsSavedMonthly)
