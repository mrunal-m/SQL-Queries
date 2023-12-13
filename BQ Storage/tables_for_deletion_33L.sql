CREATE or REPLACE table maximal-furnace-783.data_platform_temp1.final_tables_for_deletion_8Dec23
AS (
(SELECT project_id, dataset_id, table_id, table_full_name, est_monthly_cost 
from maximal-furnace-783.data_platform_temp1.unaccessed_tables_for_deletion)
UNION ALL
(SELECT project_id, dataset_id, table_id, table_full_name, est_monthly_cost
from maximal-furnace-783.data_platform_temp1.unaccessed_tables_30d_3Nov
WHERE est_monthly_cost <1)
ORDER BY 5 DESC
)
