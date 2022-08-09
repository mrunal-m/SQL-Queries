SELECT creation_time, user_email, job_id, destination_table, referenced_tables
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
  , UNNEST(referenced_tables) as referenced_tables
WHERE
  EXTRACT(DATE FROM  creation_time) >="2022-06-01"
  AND referenced_tables.dataset_id LIKE "%analytics_163194662%"
ORDER BY
  total_bytes_billed DESC;
