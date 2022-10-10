--Comparing Ondemand & FlatRate cost of a query

SELECT project_id, job_id, user_email, total_bytes_processed, total_slot_ms , 
((total_bytes_processed*5)/(1024*1024*1024*1024)) as ondemand_cost, (total_slot_ms*0.000000007716049383) as flatrate_cost,
case when ((total_slot_ms*0.000000007716049383) > ((total_bytes_processed*5)/(1024*1024*1024*1024))) then 1 else 0 end as flag, 
(total_slot_ms*0.000000007716049383) - ((total_bytes_processed*5)/(1024*1024*1024*1024)) as cost_diff
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION as i
WHERE project_id="maximal-furnace-783" AND
date(creation_time) = "2022-10-26" 
LIMIT 1000

-----
--Checking for weekly

with t1 as 
(SELECT CASE 
  WHEN (date(creation_time) BETWEEN "2022-08-01" AND "2022-08-07") then "Aug Week 1"
  WHEN (date(creation_time) BETWEEN "2022-08-08" AND "2022-08-14") then "Aug Week 2"
  WHEN (date(creation_time) BETWEEN "2022-08-15" AND "2022-08-21") then "Aug Week 3"
  WHEN (date(creation_time) BETWEEN "2022-08-22" AND "2022-08-28") then "Aug Week 4"
  WHEN (date(creation_time) BETWEEN "2022-08-29" AND "2022-09-04") then "Aug Week 5"
  WHEN (date(creation_time) BETWEEN "2022-09-05" AND "2022-09-11") then "Sept Week 1"
  WHEN (date(creation_time) BETWEEN "2022-09-12" AND "2022-09-18") then "Sept Week 2"
  WHEN (date(creation_time) BETWEEN "2022-09-19" AND "2022-09-25") then "Sept Week 3"
  WHEN (date(creation_time) BETWEEN "2022-09-26" AND "2022-10-02") then "Sept Week 4"
 END AS week, project_id,
((total_bytes_processed*5)/(1024*1024*1024*1024)) as ondemand_cost, (total_slot_ms*0.000000007716049383) as flatrate_cost,
case when ((total_slot_ms*0.000000007716049383) > ((total_bytes_processed*5)/(1024*1024*1024*1024))) then 1 else 0 end as flag, 
(total_slot_ms*0.000000007716049383) - ((total_bytes_processed*5)/(1024*1024*1024*1024)) as cost_diff, 
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION as i
WHERE date(creation_time) BETWEEN "2022-08-01" AND "2022-10-02"
)

SELECT  t1.week, t1.project_id, COUNT(*) as count, 
SUM(case when t1.flag = 0 then 1 else 0 end) as on_demand_costly,
SUM(case when t1.flag = 1 then 1 else 0 end) as flatrate_costly, 
AVG(t1.ondemand_cost) as ondemand_avg, AVG(t1.flatrate_cost) as flatrate_avg, 
approx_quantiles(t1.ondemand_cost, 100)[SAFE_ORDINAL(5)] as ondemand_p5, 
approx_quantiles(t1.flatrate_cost, 100)[SAFE_ORDINAL(5)] as flatrate_p5, 
approx_quantiles(t1.ondemand_cost, 100)[SAFE_ORDINAL(50)] as ondemand_p50, 
approx_quantiles(t1.flatrate_cost, 100)[SAFE_ORDINAL(50)] as flatrate_p50, 
approx_quantiles(t1.ondemand_cost, 100)[SAFE_ORDINAL(99)] as ondemand_p99, 
approx_quantiles(t1.flatrate_cost, 100)[SAFE_ORDINAL(99)] as flatrate_p99, 
from t1
GROUP BY 1, 2
ORDER BY 1 
