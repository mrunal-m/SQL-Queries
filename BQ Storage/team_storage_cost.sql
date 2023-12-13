with t1 as (SELECT  day, dataset_id, project_id, SUM(estimated_day_cost) from `maximal-furnace-783.data_platform_temp1`.`bq_table_storage_metadata_incremental` t
WHERE date(day) >= current_date - 3
group by 1, 2,3
order by 1), 
t2 as (select project, dataset, team_name from maximal-furnace-783.data_platform_temp1.label_dataset_team_pod_mapping)
select t1.day, t2.team_name from t1 INNER join t2 on (t1.project_id, t1.dataset_id) = (t2.project, t2.dataset)
WHERE team_name = 'ads-platform'
order by 1
