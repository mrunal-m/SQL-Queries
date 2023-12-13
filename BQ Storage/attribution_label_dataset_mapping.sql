SELECT usage_date, SUM(cost) from (with t1 as 
((Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from maximal-furnace-783.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from sharechat-production.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from moj-prod.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from sc-bigquery-product-analyst.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from sharechat-firebase.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from sharechat-migration-test.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')
UNION ALL
(Select catalog_name as project, schema_name as dataset,-- option_value,
CASE WHEN (option_value NOT LIKE  '%("id",%'AND option_value LIKE '%("dataset_id",%')  
THEN SUBSTR(SPLIT(option_value, 'dataset_id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'dataset_id')[OFFSET(1)], ',')[OFFSET(1)]) - 4)) 
WHEN (option_value LIKE '%("id",%') 
THEN SPLIT(SUBSTR(SPLIT(option_value, 'id')[OFFSET(1)] , 5, (LENGTH(SPLIT(SPLIT(option_value, 'id')[OFFSET(1)], ',')[OFFSET(1)])- 3)), '"')[OFFSET(0)]
ELSE schema_name
END label, 
from sc-bigquery-product-tools.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE OPTION_NAME = 'labels')), 

t2 as 
(SELECT usage_date, service_label_id as service_label_id, project_id, pod, team_name, pod_lead, team_owner, SUM(cost_without_any_discounts)  cost
from sc-bigquery-product-tools.penny_gauge_multi_region_us.non_cdn_view
WHERE (gcp_resource_name = "BigQuery") AND (sku_description = "Physical Storage")
AND usage_date >= current_date - 3
AND service_label_id IS NOT NULL
group by 1,2,3,4,5,6,7
)

SELECT t2.usage_date, t1.project, t1.dataset, t2.team_name,  t2.pod, t2.pod_lead, t2.team_owner, SUM(t2.cost) cost
from t1 RIGHT JOIN t2 ON (t1.project, t1.label) = (t2.project_id, t2.service_label_id)
group by 1, 2, 3,4, 5, 6, 7)
WHERE team_name = 'ads-platform'
group by 1
