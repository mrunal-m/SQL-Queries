--recheck query
WITH BQ_data AS 
(with t1 AS (SELECT CONCAT(TABLE_CATALOG, '.' , TABLE_SCHEMA, '.', TABLE_NAME) table_name, option_name, option_type, option_value
  FROM
    `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS
  WHERE
    option_name IN ('expiration_timestamp', 'partition_expiration_days')
    AND TABLE_SCHEMA IN ('sc_analytics', 'moj_analytics')),
  
  t2 AS (SELECT project_dataset_table, dataset_id, table_id,SUM(size_tb) size_tb, SUM(estimated_monthly_cost) BQ_monthly_cost 
  from maximal-furnace-783.data_platform_temp1.bq_table_storage_metadata_incremental
  WHERE day = current_date() - 1
  AND dataset_id IN ('sc_analytics', 'moj_analytics')
  group by all)

SELECT t2.project_dataset_table, dataset_id, table_id, t1.* except(table_name), t2.size_tb as BQ_size_tb, t2.BQ_monthly_cost from t1 RIGHT JOIN t2 ON (t1.table_name = t2.project_dataset_table)
),

DB_SC_Moj AS ((SELECT table_name, full_table_name, 'sc_analytics' AS dataset_BQ, SUM(total_size_gb) total_size_gb, SUM(cost_without_any_discounts) DB_cost 
from maximal-furnace-783.databricks_attribution.sc_storage_attribution
WHERE date  BETWEEN "2024-08-15" AND "2024-09-14"
AND CATALOG_NAME IN ('sharechat_prod_perm_global', 'platforms_prod_perm_global')
GROUP BY ALL)
UNION ALL

 (SELECT table_name, full_table_name,'moj_analytics' AS dataset_BQ,  SUM(total_size_gb) total_size_gb, SUM(cost_without_any_discounts) DB_cost 
from maximal-furnace-783.databricks_attribution.moj_storage_attribution
WHERE date BETWEEN "2024-08-15" AND "2024-09-14"
AND CATALOG_NAME IN ('moj_prod_perm_global', 'platforms_prod_perm_global')
GROUP BY ALL)
)
--full_table_name

SELECT BQ_data.* except(option_type, dataset_id, table_id), DB_SC_Moj.full_table_name, DB_SC_Moj.total_size_gb, DB_SC_Moj.DB_cost
from BQ_data LEFT JOIN DB_SC_Moj
ON (BQ_data.dataset_id, BQ_data.table_id) = (DB_SC_Moj.dataset_BQ, DB_SC_Moj.table_name)
