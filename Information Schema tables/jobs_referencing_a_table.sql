SELECT date(creation_time), job_id from region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION ,
UNNEST(referenced_tables) as referenced_tables
WHERE date(creation_time) >= current_date() - 30
  AND referenced_tables.project_id LIKE "maximal-furnace-783"
  AND referenced_tables.dataset_id LIKE "moj_analytics"
  AND referenced_tables.table_id LIKE "sc_content_asia_usage_logs"
