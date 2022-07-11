SELECT timestamp_trunc(timestamp, HOUR) as hour, SUM(total_rows) FROM 
    `maximal-furnace-783.region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_TIMELINE_BY_ORGANIZATION
  WHERE
    date(timestamp) >= '2022-07-06' and date(timestamp) <= '2022-07-08'
    and TABLE_NAME = 'session_time_hourly'
    GROUP BY 1
order by 1
