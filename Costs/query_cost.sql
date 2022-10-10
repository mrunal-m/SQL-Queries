--Comparing Ondemand & FlatRate Costs for random queries
SELECT project_id, job_id, user_email, total_bytes_processed, total_slot_ms , 
((total_bytes_processed*5)/(1024*1024*1024*1024)) as ondemand_cost, (total_slot_ms*0.000000007716049383) as flatrate_cost,
case when ((total_slot_ms*0.000000007716049383) > ((total_bytes_processed*5)/(1024*1024*1024*1024))) then 1 else 0 end as flag, 
(total_slot_ms*0.000000007716049383) - ((total_bytes_processed*5)/(1024*1024*1024*1024)) as cost_diff
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION as i
WHERE project_id="maximal-furnace-783" AND
date(creation_time) = "2022-10-26" 
LIMIT 1000
