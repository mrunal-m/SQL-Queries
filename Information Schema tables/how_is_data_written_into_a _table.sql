-- Method 1: Streaming Insert

SELECT * from `region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_ORGANIZATION
WHERE dataset_id LIKE "%data_extraction%" AND table_id LIKE "livestream_notification_cohort"
AND (date(start_timestamp) BETWEEN "2022-11-01" AND "2022-11-02")

--Method 2&3: Batch Loads & Scheduled Queries

SELECT  creation_time, job_id, priority, job_type, total_slot_ms, destination_table, referenced_tables,
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION as i
WHERE destination_table.dataset_id LIKE "data_extraction" 
AND destination_table.table_id LIKE "livestream_notification_cohort"
AND (date(creation_time, "Asia/Kolkata") BETWEEN "2022-10-30" AND "2022-11-02")
ORDER BY 1 ASC
