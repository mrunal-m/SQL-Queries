with t1 AS (SELECT CONCAT(TABLE_CATALOG, '.' , TABLE_SCHEMA, '.', TABLE_NAME) table_name, option_name, option_type, option_value
  FROM
    `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS
  WHERE
    option_name IN ('expiration_timestamp', 'partition_expiration_days')
    AND TABLE_SCHEMA IN ('sc_analytics', 'moj_analytics')),
  
  t2 AS (SELECT project_dataset_table, table_id, SUM(size_tb) size_tb, SUM(estimated_monthly_cost) BQ_monthly_cost 
  from maximal-furnace-783.data_platform_temp1.bq_table_storage_metadata_incremental
  WHERE day = current_date() - 1
  AND dataset_id IN ('sc_analytics', 'moj_analytics')
  group by all)

SELECT t2.project_dataset_table, t1.* except(table_name), t2.size_tb as BQ_size_tb, t2.BQ_monthly_cost from t1 RIGHT JOIN t2 ON (t1.table_name = t2.project_dataset_table)
order by 6 desc
