With t1 AS (SELECT distinct date(creation_time, "Asia/Kolkata") dt,
job_id, MAX(case when (referenced_tables.project_id = 'maximal-furnace-783'
      AND referenced_tables.dataset_id IN ('moj_analytics', 'sc_analytics')) then 1
      else 0 end) raw_table_indicator,
      CASE WHEN ((job_id LIKE "%airflow%") OR parent_job_id LIKE "%airflow%") THEN "airflow"
    WHEN (user_email IN ('superset-bi-tool-sa@prj-sc-p-services-24df.iam.gserviceaccount.com',
          'monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com',
          'redash@maximal-furnace-783.iam.gserviceaccount.com',
          'metabase-sa@maximal-furnace-783.iam.gserviceaccount.com',
          'information-schema-sa@sc-bigquery-product-tools.iam.gserviceaccount.com', 
          'retool-sa@sc-bigquery-product-tools.iam.gserviceaccount.com') 
          OR user_email LIKE "%metabase%" OR user_email LIKE "%locale%") THEN "bi_tool"
    WHEN (user_email LIKE "%bigquerydatatransfer%") THEN "data_transfer"
    WHEN ((job_id LIKE "%scheduled%" AND job_id NOT LIKE "%dataconnector%")) THEN "scheduled_queries"
    WHEN (job_id LIKE "%dataconnector%") THEN "gsheets_data_connector"
    WHEN (job_id LIKE "%beam%") THEN "bq_dataflow"
    WHEN ((job_id LIKE "%bquxjob%") OR (job_id LIKE "%script%" AND statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'CREATE_TABLE_AS_SELECT') AND user_email NOT LIKE "%gservice%")) THEN "adhoc_bq_console"
    WHEN (user_email IN ('sink-loader@prj-sc-p-databricks.iam.gserviceaccount.com',
        'sink-loader@prj-moj-p-databricks.iam.gserviceaccount.com',
        'bq-db-exporter@prj-sc-p-databricks.iam.gserviceaccount.com',
        'bq-db-exporter@prj-moj-p-databricks.iam.gserviceaccount.com')) THEN "batch_uploads"
    WHEN (user_email LIKE "%gservice%" AND (job_id NOT LIKE "%bqux%" AND job_id NOT LIKE "%scheduled%"  AND parent_job_id NOT LIKE "%scheduled%"
    AND job_id NOT LIKE "%beam%" AND job_id NOT LIKE "%dataconnector%" AND job_id NOT LIKE "%airflow%" AND job_id LIKE "%script%")) THEN "bq_code_script"
    ELSE "others" END AS job_type, 
      total_slot_ms
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION i, UNNEST(referenced_tables) referenced_tables
  WHERE (date(creation_time, "Asia/Kolkata") >= current_date("Asia/Kolkata")-10)
  AND (statement_type IS NULL OR UPPER(statement_type) NOT IN ("SCRIPT") )
  GROUP BY ALL
  ORDER BY 1 ASC)

  SELECT t1.dt, t1.raw_table_indicator, job_type, COUNT(distinct job_id) count_jobs, SUM(total_slot_ms)/(1000*3600*24) slot_day
  from t1
  GROUP BY ALL
  ORDER BY 1 ASC
