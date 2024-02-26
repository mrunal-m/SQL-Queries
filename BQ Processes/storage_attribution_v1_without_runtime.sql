--CREATE OR REPLACE table maximal-furnace-783.data_platform_temp1.storage_attribution_v1
--AS 
(
-- non_sc_moj as 
(
with  t1 as 
(SELECT distinct usage_date, project_id, REPLACE(service_label_id, '-', '_') dataset_id, team_name, NULL pod-- (SUM(cost_without_any_discounts)*30) PG_cost
from `sc-bigquery-product-tools.penny_gauge_multi_region_us.non_cdn_view`
WHERE `gcp_resource_name` IN ('BigQuery')
  AND `service` IN ('bigquery-storage-service')
  AND `component` IN ('bigquery-storage')
  AND `sku_description` IN ('Physical Storage')
  AND service_label_id NOT IN ('moj-analytics', 'sc-analytics')
AND usage_date = current_date() - 2
--GROUP BY 1, 2, 3, 4, 5
),

t2 as ( 
SELECt distinct day, project_id, dataset_id, table_id, 
SUM(estimated_monthly_cost) Metadata_cost
FROM maximal-furnace-783.data_platform_temp1.bq_table_storage_metadata_incremental 
WHERE day = current_date() - 2
GROUP BY 1, 2, 3, 4
)

SELECT distinct t1.usage_date,  t1.project_id, t1.dataset_id, t2.table_id,t1.team_name, SAFE_CAST(t1.pod AS STRING) pod, t2.Metadata_cost est_monthly_cost
--t2.* except (day, project_id, dataset_id)
from t1 INNER JOIN  t2
ON (t1.project_id = t2.project_id AND t1.dataset_id = t2.dataset_id AND t1.usage_date = t2.day)
WHERE t1.usage_date = current_date() - 2
AND t2.day = current_date() - 2
order by 6 desc 
)

UNION ALL

--sc_moj_analytics as 
(with a as (SELECT tt2.day, tt1.project_id, tt1.dataset_id, tt1.table_id, tt1.pod, tt1.entity, SUM(tt2.estimated_monthly_cost) est_monthly_cost 
from maximal-furnace-783.data_platform_temp1.dataset_table_label_mapping tt1
INNER JOIN maximal-furnace-783.data_platform_temp1.bq_table_storage_metadata_incremental tt2
ON (tt1.project_id = tt2. project_id AND tt1.dataset_id = tt2.dataset_id AND tt1.table_id = tt2.table_id)
WHERE date(day) = current_date() - 2
group by 1, 2, 3, 4, 5, 6), 
b as (SELECT project_id, REPLACE(service_label_id, '-', '_') dataset_id, team_name, pod, entity 
from sc-bigquery-product-tools.penny_gauge_multi_region_us.non_cdn_view
WHERE date(usage_date) = current_date() - 2
AND service_label_id IN ('moj-analytics', 'sc-analytics'))

SELECT distinct a.day usage_date, a.project_id, a.dataset_id, a.table_id, b.team_name, a.pod, a.est_monthly_cost
from a LEFT JOIN  b 
ON (a.project_id = b.project_id AND a.dataset_id = b.dataset_id AND a.entity = b.entity AND a.pod = b.pod)
)
)
