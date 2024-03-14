CREATE OR REPLACE TABLE maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024_dupe
AS (
WITH t1 AS (
select project_dataset_table, estimated_monthly_cost 
from `maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024`
ORDER BY estimated_monthlY_cost DESC),

t2 AS (SELECT creation_time , user_email, CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) table_full_name
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION, UNNEST(referenced_tables) rt
WHERE date(creation_time) >= current_date() - 600),

t3 AS (SELECT t2.*, RANK() OVER(partition by table_full_name order by creation_time desc) as rank_1 from t2) 

SELECT DISTINCT t1.*, t3.creation_time last_accessed_on, t3.user_email last_user, 
from t1 LEFT JOIN t3 ON t1.project_dataset_table = t3.table_full_name
AND t3.rank_1=1
ORDER BY estimated_monthly_cost DESC
)
