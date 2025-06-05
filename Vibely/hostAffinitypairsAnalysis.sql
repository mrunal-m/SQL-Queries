-- SELECT date(date) dt, hour, hostUserId, count(distinct userId) FROM 
-- `maximal-furnace-783.vibely_analytics.hostAffinityPairs`
-- WHERE date(date) = current_date() AND LOWER(affinity) = 'video'
-- group by all
-- order by 4 desc 
SELECT APPROX_QUANTILES(countUsers, 100)[offset(50)] P50,
APPROX_QUANTILES(countUsers, 100)[offset(70)] p70,
APPROX_QUANTILES(countUsers, 100)[offset(80)] p80,
APPROX_QUANTILES(countUsers, 100)[offset(90)] p90,
APPROX_QUANTILES(countUsers, 100)[offset(97)] p97,

AVG(countUsers) avg
FROM (
SELECT hostUserId, count(distinct userId) countUsers
 FROM `maximal-furnace-783.vibely_analytics.hostAffinityPairs`
WHERE 
date(date) = current_date()
group by 1
)
