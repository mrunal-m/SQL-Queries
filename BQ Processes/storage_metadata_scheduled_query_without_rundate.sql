WITH
  tables AS (
  SELECT
    t.project_id,
    t.table_schema as dataset_id,
    t.table_name  as table_id,
    CASE
        WHEN DATE(i.creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 90 DAY) THEN 1
      ELSE
      0
    END as created_within_90_days,
    (total_physical_bytes)/POW(1024,4) size_tb,
    ((total_physical_bytes)/POW(1024,4))*1024*81*0.026 estimated_monthly_cost
  FROM (
    SELECT
      *
    FROM
      `sharechat-production.region-us`.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT
      *
    FROM
      `sharechat-firebase.region-us`.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT
      *
    FROM
      `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT
table_catalog,
table_schema,
table_name,
table_type,
is_insertable_into,
is_typed,
creation_time,
base_table_catalog,
base_table_schema,
base_table_name,
snapshot_time_ms,
ddl,
default_collation_name,
upsert_stream_apply_watermark
    FROM
      `moj-prod.region-us`.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT
      *
    FROM
      `sc-bigquery-product-analyst.region-us`.INFORMATION_SCHEMA.TABLES) i
  JOIN
    `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION t
  ON
    (t.project_id,
      t.table_schema,
      t.table_name)=(i.table_catalog,
      i.table_schema,
      i.table_name)
 ),

  referenced_tables AS (
  SELECT
    DISTINCT ref.table_id,
    ref.dataset_id,
    ref.project_id AS project_id_ref,
    MAX(CASE
        WHEN DATE(creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 90 DAY) THEN 1
      ELSE
      0
    END
      ) accessed_last_90_days
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION j,
    UNNEST(referenced_tables) ref
  WHERE
    DATE(creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 90 DAY)
  GROUP BY
    1,
    2,
    3 ),

  creators AS (
  SELECT
    DISTINCT project_id,
    dataset_id,
    table_id,
    owner
  FROM (
    SELECT
      DISTINCT destination_table.project_id AS project_id,
      destination_table.dataset_id AS dataset_id,
      destination_table.table_id AS table_id,
      user_email AS owner,
      RANK() OVER (PARTITION BY destination_table.project_id, destination_table.dataset_id, destination_table.table_id ORDER BY creation_time DESC) AS rnk
    FROM
      `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE
      user_email IS NOT NULL
    GROUP BY
      1,
      2,
      3,
      4,
      creation_time
    ORDER BY
      1,
      2,
      3,
      5 DESC)
  WHERE
    rnk=1 )

SELECT
  DISTINCT 
  CURRENT_DATE("Asia/Kolkata") as day,
  t.project_id,
  t.dataset_id,
  concat(t.project_id,'.',t.dataset_id) project_dataset,
  t.table_id,
    concat(t.project_id,'.',t.dataset_id,'.',t.table_id) project_dataset_table,
  t.size_tb,
  CASE
    WHEN owner IS NULL THEN 'No owner'
  ELSE
  owner
END
  AS Table_owner,
  t.estimated_monthly_cost,
    t.estimated_monthly_cost/30 as estimated_day_cost,
  CASE
    WHEN accessed_last_90_days IS NULL THEN 0
  ELSE
  accessed_last_90_days
END
  AS accessed_last_90_days,
  created_within_90_days,
  pod_lead,
  pod_lead_email,
  CASE
    WHEN BU_name IS NULL THEN 'Others'
  ELSE
  BU_name
END
  AS BU_name
FROM
  tables t
LEFT JOIN
  creators c
ON
  (t.project_id,
    t.dataset_id,
    t.table_id ) = (c.project_id,
    c.dataset_id,
    c.table_id)
LEFT JOIN
  referenced_tables r
ON
  (t.project_id,
    t.dataset_id,
    t.table_id ) = (r.project_id_ref,
    r.dataset_id,
    r.table_id)
LEFT JOIN
  maximal-furnace-783.data_platform_temp1.user_pod_lead_mapping u
ON
  owner=u.email
