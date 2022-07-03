SELECT invoice.month as month, project.id as project, service.description, sku.description,
SUM(ROUND(CAST(cost AS NUMERIC),2)) as cost
FROM `sharechat-production.sharechat_billing_dataset.gcp_billing_export_v1_015E7D_9BBFF6_B1E1D1`
WHERE (DATE(usage_start_time) BETWEEN '2022-04-01' AND '2022-06-28')
AND UPPER(service.description) LIKE "%BIGQUERY STORAGE API%"
GROUP BY 1, 2, 3, 4
ORDER BY 1
