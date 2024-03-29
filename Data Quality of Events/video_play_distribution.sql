
(select date(time), 
"duration" as column,
COUNT (*) as total_rows,
COUNT (Case when duration IS NULL then 1 end) as null_count,
COUNT (case when duration IS NOT NULL then 1 end) unique,
MIN(duration) as minimum,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(5)] as p5,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(25)] as p25,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(50)] as p50,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(75)] as p75,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(90)] as p90,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(95)] as p95,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(99)] as p99,
  approx_quantiles(duration, 100)[SAFE_ORDINAL(100)] as maximimum
  from `maximal-furnace-783.sc_analytics.video_play`
  WHERE
time>='2022-07-25' AND time<="2022-07-31"
GROUP BY 1
)

UNION ALL
(select date(time), 
"timeSpent" as column,
COUNT (*) as total_rows,
COUNT (Case when duration IS NULL then 1 end) as null_count,
COUNT (case when duration IS NOT NULL then 1 end) unique,
MIN(SAFE_CAST(timeSpent AS INT64)) as minimum,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(5)] as p5,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(25)] as p25,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(50)] as p50,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(75)] as p75,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(90)] as p90,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(95)] as p95,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(99)] as p99,
  approx_quantiles(SAFE_CAST(timeSpent AS INT64), 100)[SAFE_ORDINAL(100)] as maximimum
  from `maximal-furnace-783.sc_analytics.video_play`
  WHERE
time>='2022-07-25' AND time<="2022-07-31"
GROUP BY 1
)

UNION ALL
(select date(time), 
"percentage" as column,
COUNT (*) as total_rows,
COUNT (Case when percentage IS NULL then 1 end) as null_count,
COUNT (case when percentage IS NOT NULL then 1 end) unique,
MIN(percentage) as minimum,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(5)] as p5,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(25)] as p25,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(50)] as p50,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(75)] as p75,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(90)] as p90,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(95)] as p95,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(99)] as p99,
  approx_quantiles(percentage, 100)[SAFE_ORDINAL(100)] as maximimum
  from `maximal-furnace-783.sc_analytics.video_play`
  WHERE
time>='2022-07-25' AND time<="2022-07-31"
GROUP BY 1
)

UNION ALL
(select date(time), 
"percentageFloat" as column,
COUNT (*) as total_rows,
COUNT (Case when percentageFloat IS NULL then 1 end) as null_count,
COUNT (case when percentageFloat IS NOT NULL then 1 end) unique,
MIN(percentageFloat) as minimum,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(5)] as p5,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(25)] as p25,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(50)] as p50,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(75)] as p75,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(90)] as p90,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(95)] as p95,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(99)] as p99,
  approx_quantiles(percentageFloat, 100)[SAFE_ORDINAL(100)] as maximimum
  from `maximal-furnace-783.sc_analytics.video_play`
  WHERE
time>='2022-07-25' AND time<="2022-07-31"
GROUP BY 1
)


UNION ALL
(select date(time), 
"videoStartTime" as column,
COUNT (*) as total_rows,
COUNT (Case when videoStartTime IS NULL then 1 end) as null_count,
COUNT (case when videoStartTime IS NOT NULL then 1 end) unique,
MIN(videoStartTime) as minimum,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(5)] as p5,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(25)] as p25,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(50)] as p50,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(75)] as p75,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(90)] as p90,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(95)] as p95,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(99)] as p99,
  approx_quantiles(videoStartTime, 100)[SAFE_ORDINAL(100)] as maximimum
  from `maximal-furnace-783.sc_analytics.video_play`
  WHERE
time>='2022-07-25' AND time<="2022-07-31"
GROUP BY 1
)
