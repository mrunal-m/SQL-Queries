-- Pricing Buckets

With master_data as
(SELECT project.id as project_id,
date(usage_start_time) as date,
date(date_trunc(usage_start_time,WEEK)) as week,
DATE(usage_start_time) as day, 
service.description as service_description, 
(case when sku.description like 'Streaming%' then 'Streaming Insert'
when sku.description like 'Analysis%' then 'Analysis'
when sku.description like 'BigQuery Flat Rate Flex%' then 'Flex Rate'
when sku.description like 'BigQuery Flat Rate Monthly%' then 'Flat Rate Monthly'
when sku.description like 'Physical Storage%' then 'Physical Storage'
else 'Misc' end) as sk_cost_bucket,
sku.description as sku_description, SUM(ROUND(CAST(cost AS NUMERIC),2)) as cost
FROM `sharechat-production.sharechat_billing_dataset.gcp_billing_export_v1_015E7D_9BBFF6_B1E1D1`
WHERE DATE(usage_start_time) > '2022-06-01'
AND service.description IN ('BigQuery',
      'BigQuery Storage API',
      'BigQuery Reservation API')
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3,4,5,5,7,8 desc)


Select date, week, project_id, sk_cost_bucket, sum(cost) total_cost
from master_data
group by 1,2,3,4
order by 1 desc, 3 desc
