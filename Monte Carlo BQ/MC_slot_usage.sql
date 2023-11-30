SELECT  date(creation_time) dt, TIMESTAMP_TRUNC(creation_time, HOUR) hr, 
SUM(total_slot_ms)/(1000*60*60*24) as slot_day, 
from prj-sc-p-montecarlo-service.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE date(creation_time) >=current_date-2
AND user_email = "monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com"
AND error_result.reason IS NULL
group by 1, 2
order by 1, 2, 3 desc
