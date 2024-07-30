WITH t1 AS (SELECT distinct date(creation_time, "Asia/Kolkata") dt, project_id, user_email, job_id, total_slot_ms,
TIMESTAMP_DIFF(end_time, start_time, second) runtime
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION 
WHERE date(creation_time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") -21
AND job_id LIKE "%scheduled%" AND (statement_type IS NULL OR UPPER(statement_type) NOT IN ("SCRIPT"))
),

t2 AS (SELECT distinct owner_email_id, scheduled_query_name, transfer_config_id, refresh_schedule 
FROM maximal-furnace-783.data_platform_temp1.scheduled_queries_meta
 WHERE date(ingest_time, "Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") -21
 AND disabled IS FALSE),

t3 AS (SELECT distinct OwnerEmail, TransferConfigID, BQJobID from maximal-furnace-783.data_platform_temp1.scheduled_query_runs),

t4 AS (SELECT distinct t2.owner_email_id,t2.scheduled_query_name, t2.refresh_schedule, t3.BQJobID from t2 INNER JOIN t3 ON
( t2.transfer_config_id) = ( t3.transferConfigID))

SELECT DISTINCT t1.dt, t1.project_id, t1.user_email, t4.scheduled_query_name, t4.refresh_schedule, --job_id, runtime, total_slot_ms
COUNT(distinct job_id) count_jobs_per_day, (SUM(runtime)/COUNT(distinct job_id)) avg_runtime_per_run, SUM(t1.total_slot_ms)/(1000*3600*24) slot_day
from t1 INNER JOIN t4 ON (t1.job_id) = (t4.BQJobID)
group by all
order by 1 ASC
