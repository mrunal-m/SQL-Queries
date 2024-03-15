CREATE OR REPLACE TABLE maximal-furnace-783.data_platform_temp1.events_to_deprecate_15March2024
AS (
  SELECT * from maximal-furnace-783.data_platform_temp1.deleted_event_tables_15March2024 t1
  WHERE (t1.table_name NOT IN (SELECT t2.table_name from maximal-furnace-783.data_platform_temp1.whitelisted_event_tables_14March2024 t2
  WHERE t2.table_name IS NOT NULL))
)
