-- SLot Usage for SA = monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com
SELECT date(creation_time, "Asia/Kolkata") as dt,
COUNT(*) as count,
SUM(total_slot_ms)/(1000*60*60*24) as slot_day, 
from `prj-sc-p-montecarlo-service.region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE date(creation_time, "Asia/Kolkata") >="2023-10-25"
AND user_email = "monte-carlo@prj-sc-p-montecarlo-service.iam.gserviceaccount.com"
GROUP BY 1
ORDER BY 1 ASC 
