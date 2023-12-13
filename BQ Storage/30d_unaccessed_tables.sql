--Tables older than 30d not accessed in 30d
--447868
SELECT * from (
select project_id, dataset_id, table_id, sum(estimated_monthly_cost) est_monthly_cost 
from (WITH
  tables AS (
  SELECT
    t.project_id,
    t.table_schema as dataset_id,
    t.table_name  as table_id,
    CASE
        WHEN DATE(i.creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 30 DAY) THEN 1
      ELSE
      0
    END as created_within_30_days,
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
      `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT
      *
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
        WHEN DATE(creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 30 DAY) THEN 1
      ELSE
      0
    END
      ) accessed_last_30_days
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION j,
    UNNEST(referenced_tables) ref
  WHERE
    DATE(creation_time,"Asia/Kolkata")>=DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 30 DAY)
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

Select t.*, pod, team, pod_lead, team_owner

from
(SELECT
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
    WHEN accessed_last_30_days IS NULL THEN 0
  ELSE
  accessed_last_30_days
END
  AS accessed_last_30_days,
  created_within_30_days,

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
  owner=u.email) t
left join `maximal-furnace-783.data_platform_temp1.dataset_pod_lead_mapping_temp` m on (catalog_name, schema_name)=(project_id, dataset_id)
left join maximal-furnace-783.cost_saving.whitelisted_unused_tables u on (t.project_id,t.dataset_id, t.table_id)=(u.project_id,u.dataset_id, u.table_id)
left join maximal-furnace-783.cost_saving.deleted_unused_tables d on (t.project_id,t.dataset_id, t.table_id)=(d.project_id,d.dataset_id, d.table_id)
where accessed_last_30_days=0 and created_within_30_days=0 and u.table_id is null)

GROUP BY 1, 2,3)
WHERE (est_monthly_cost BETWEEN 0.00 AND 1.00)
ORDER by 4 desc
