CREATE OR REPLACE TABLE maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024
AS (SELECT * from maximal-furnace-783.data_platform_temp1.unaccessed_tables_21d_4March2024_dupe
ORDER BY estimated_monthly_cost DESC)
