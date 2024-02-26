--CREATE OR REPLACE table maximal-furnace-783.data_platform_temp1.storage_attribution_tempv2_23Feb2024
 --AS 
 (
WITH t1 as
((SELECT distinct * from  maximal-furnace-783.data_platform_temp1.storage_attribution_temp_23Feb2024 a
WHERE a.team_name IS NOT  NULL
ORDER BY 7 desc)

UNION ALL

(SELECT distinct a.usage_date,
a.project_id,
a.dataset_id,
a.table_id,
b.team_name,
a.pod,
a.est_monthly_cost from  maximal-furnace-783.data_platform_temp1.storage_attribution_temp_23Feb2024 a
INNER JOIN  sc-bigquery-product-tools.penny_gauge_multi_region_us.pod_owners b
ON a.pod = b.current_pod_name
WHERE a.team_name IS NULL 
ORDER BY 7 desc))

SELECT t2.day usage_date, t2.project_id, t2.dataset_id, t2.table_id, t1.team_name, t1.pod, t2.estimated_monthly_cost
from maximal-furnace-783.data_platform_temp1.bq_table_storage_metadata_incremental t2
LEFT JOIN t1 ON (t1.project_id = t2.project_id AND t1.dataset_id = t2.dataset_id AND t1.table_id = t2.table_id)
WHERE date(day) =  DATE_SUB(@run_date, INTERVAL 2 DAY)
--ORDER BY 7 desc
)
