SELECT * from 
(with t1 as 
(SELECT date(creation_time, "Asia/Kolkata") as dt, 
query, case 
when (query like "%field_health%") then "FHM"
when (query like "%custom_sql%") then "custom_sql"
when (query like "%field_quality%") then "FQ"
when (query like "%category_dist%") then "DT"
when (query like "%_TABLES_%") then 'metadata'
else "others" end Monitor_type,
CONCAT(referenced_tables.project_id, ".", referenced_tables.dataset_id, ".", referenced_tables.table_id) table,
error_result.reason err,
total_slot_ms
from prj-sc-p-montecarlo-service.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, UNNEST(referenced_tables) referenced_tables
WHERE date(creation_time, "Asia/Kolkata") >= "2023-11-29"
AND user_email = "monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com")

SELECT t1.dt, query,
case when (Monitor_type IN ("FHM", "DT")) THEN LEFT(RIGHT(substring(t1.query, STRPOS(t1.query, '"monitor_uuid":'), 54), 37), 36)
when (Monitor_type IN ("custom_sql", "FQ")) THEN LEFT(RIGHT(substring(query, STRPOS(query, '{"uuid":'), 47), 37), 36)
ELSE NULL END UUID
,
t1.Monitor_type,
SUM(total_slot_ms)/(1000*60*60*24) as slot_day,
SUM(CASE WHEN (err IS NOT NULL) then 1 else 0 end) failed_queries,
SUM(CASE WHEN (err IS NULL) then 1 else 0 end) successful_queries,
from t1
group by 1,2, 3, 4)
ORDER BY 1 ASC, 4 DESC
