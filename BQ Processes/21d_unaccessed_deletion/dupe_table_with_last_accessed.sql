CREATE OR REPLACE TABLE maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024_dupe
AS (with t1 AS
(SELECT * from maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024),
t2 AS
(SELECT project_dataset_table, last_accessed_on, last_user from maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024_dupe)

SELECT t1.*, t2.* except (project_dataset_table)
from t1 INNER JOIN t2 ON t1.project_dataset_table = t2.project_dataset_table
ORDER BY estimated_monthly_cost DESC)
