SELECT date, job_type, SUM(slot_day) slot_day
FROM (WITH t1 AS (SELECT DATE(creation_time, "Asia/Kolkata") AS date, job_id, project_id,  reservation_id, i.user_email, priority,
    CASE 
    WHEN ((job_id LIKE "%airflow%") OR parent_job_id LIKE "%airflow%") THEN "airflow"
    WHEN (i.user_email IN ('superset-bi-tool-sa@prj-sc-p-services-24df.iam.gserviceaccount.com',
          'monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com',
          'redash@maximal-furnace-783.iam.gserviceaccount.com',
          'metabase-sa@maximal-furnace-783.iam.gserviceaccount.com',
          'information-schema-sa@sc-bigquery-product-tools.iam.gserviceaccount.com', 
          'retool-sa@sc-bigquery-product-tools.iam.gserviceaccount.com', 
          'tns-locale-bq-alerts@sharechat-production.iam.gserviceaccount.com',
          'information-schema-sa@sc-bigquery-product-tools.iam.gserviceaccount.com') 
          OR starts_with(job_id, 'job_')
          OR (i.user_email LIKE "%metabase%" OR i.user_email LIKE "%locale%" OR i.user_email LIKE "%redash%")) THEN "bi_tool"
    WHEN (i.user_email LIKE "%bigquerydatatransfer%") THEN "data_transfer"
    WHEN ((job_id LIKE "%scheduled%" AND job_id NOT LIKE "%dataconnector%") OR parent_job_id LIKE "%scheduled%" ) THEN "scheduled_queries"
    WHEN (job_id LIKE "%dataconnector%") THEN "gsheets_data_connector"
    WHEN (job_id LIKE "%beam%") THEN "bq_dataflow"
    WHEN ((job_id LIKE "%bquxjob%") OR (job_id LIKE "%script%" AND statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'CREATE_TABLE_AS_SELECT') AND user_email NOT LIKE "%gservice%")) THEN "adhoc_bq_console"
    WHEN (i.user_email IN ('sink-loader@prj-sc-p-databricks.iam.gserviceaccount.com',
        'sink-loader@prj-moj-p-databricks.iam.gserviceaccount.com',
        'bq-db-exporter@prj-sc-p-databricks.iam.gserviceaccount.com',
        'bq-db-exporter@prj-moj-p-databricks.iam.gserviceaccount.com')) THEN "batch_uploads"
    WHEN starts_with(job_id, 'bqts_') THEN "data_transfers"
    WHEN (i.user_email LIKE "%gservice%" AND ((regexp_contains(job_id, '^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$') OR 
    regexp_contains(job_id, '^[A-Za-z0-9]{27}$')) OR (regexp_contains(parent_job_id, '^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$') OR 
    regexp_contains(parent_job_id, '^[A-Za-z0-9]{27}$'))) ) THEN "bq_code_script"
    ELSE "others" END AS job_type, 
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION i 
  WHERE DATE(creation_time, "Asia/Kolkata") >= "2024-08-20"
    AND (UPPER(statement_type)!='SCRIPT' or statement_type is null )
  GROUP BY ALL),

  t2 AS (SELECT DATE(creation_time,"Asia/Kolkata") day,
    EXTRACT(HOUR FROM DATETIME(creation_time,"Asia/Kolkata")) AS IST_hour, job_id,
    total_slot_ms,
    TIMESTAMP_DIFF(end_time, start_time, second) duration,
    SUM(total_slot_ms)/(1000*60*60*24) AS slot_day,
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
  WHERE DATE(creation_time, "Asia/Kolkata") >= "2024-08-05"
  AND (UPPER(statement_type)!='SCRIPT' or statement_type is null)
  GROUP BY ALL),

res as (
select distinct * from (
  select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `prj-mt-common-p-bq-admin.region-us`.INFORMATION_SCHEMA.RESERVATIONS
  union all
    select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `sc-bigquery-product-analyst.region-us`.INFORMATION_SCHEMA.RESERVATIONS
  union all
    select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.RESERVATIONS
  union all
    select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `moj-prod.region-us`.INFORMATION_SCHEMA.RESERVATIONS
  union all
    select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `sharechat-production.region-us`.INFORMATION_SCHEMA.RESERVATIONS 
  union all
    select CONCAT(project_id, ":", "US", ".", reservation_name) as reservation_id, edition from `moj-stag.region-us`.INFORMATION_SCHEMA.RESERVATIONS)
)

SELECT t1.*,t2.IST_hour, total_slot_ms, 
  SUM(duration) duration,
  SUM(slot_day) AS slot_day,
  res.edition,

FROM t1 INNER JOIN t2
ON t1.job_id =t2.job_id
LEFT JOIN res ON t1.reservation_id = res.reservation_id

GROUP BY all
 )
 GROUP BY ALL
 
--WHERE job_type LIKE "%others%"
