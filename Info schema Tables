--Finding Drop & Delete Queries in sc-prod project

SELECT
  date(start_time) as date, project_id, user_email, query ,statement_type, priority, start_time, end_time
FROM `sharechat-production.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE project_id = 'sharechat-production'
  and (date(start_time) IN ("2022-06-10" , "2022-06-22"))
  AND (statement_type LIKE '%DROP%' OR statement_type LIKE "%DELETE%")
