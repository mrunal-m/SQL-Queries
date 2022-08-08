--Big tables by data streamed into it
SELECT
  project_id,
  dataset_id,
  table_id,
  SUM(total_rows) AS num_rows,
  SUM(total_input_bytes) AS num_bytes,
FROM
  sharechat-firebase.`region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
  WHERE project_id="sharechat-firebase"
  AND date(start_timestamp, "Asia/Kolkata")="2022-08-04"
GROUP BY
1,2,3
ORDER BY
  num_bytes DESC;
