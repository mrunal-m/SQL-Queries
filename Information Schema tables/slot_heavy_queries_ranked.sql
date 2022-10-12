WITH t1 as (SELECT date(creation_time) as dt, creation_time, project_id, job_id, user_email, total_bytes_processed, total_slot_ms, 
end_time - start_time as duration, priority,
RANK() OVER (PARTITION BY date(creation_time), project_id ORDER BY total_slot_ms DESC) as ranks
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION 
WHERE date(creation_time) BETWEEN "2022-09-26" AND "2022-10-09" 
AND project_id IN ('sc-bigquery-product-analyst', 'sc-bigquery-product-tools', 'sc-notification-creation','sharechat-product-only')
)
SELECT * from t1
WHERE t1.ranks <=50
ORDER BY 1 ASC, 10 ASC
