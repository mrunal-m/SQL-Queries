SELECT service.description, sku.description, usage_start_time, usage_end_time, project.id, project.name,
location.location, location.region, usage.amount_in_pricing_units, usage.pricing_unit, 
invoice.month, cost_type, cost	
from  sharechat-production.sharechat_billing_dataset.gcp_billing_export_v1_015E7D_9BBFF6_B1E1D1 
WHERE UPPER(sku.description) LIKE "%STORAGE API%"
AND date(usage_start_time) >= "2022-06-01"
