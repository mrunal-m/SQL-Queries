SELECT
DATE(time),
count(case when name is null then 1 end) as name,
count(case when distinct_id is null then 1 end) as distinct_id,
count(case when downloadSpeed is null then 1 end) as downloadSpeed,
count(case when referrer is null then 1 end) as referrer,
count(case when radio is null then 1 end) as radio,
count(case when type is null then 1 end) as type,
count(case when appVersion is null then 1 end) as appVersion,
count(case when deviceId is null then 1 end) as deviceId,
count(case when clientType is null then 1 end) as clientType,
count(case when deviceModel is null then 1 end) as deviceModel,
count(case when ip is null then 1 end) as ip,
count(case when osVersion is null then 1 end) as osVersion,
count(case when ntp_eventRecordTime is null then 1 end) as ntp_eventRecordTime,
count(case when ntp_eventDispatchTime is null then 1 end) as ntp_eventDispatchTime,
count(case when sequence_number is null then 1 end) as sequence_number,
count(case when serverTime is null then 1 end) as serverTime,
count(case when sessionId is null then 1 end) as sessionId,
count(case when advertisingId is null then 1 end) as advertisingId,
count(case when tenant is null then 1 end) as tenant,

FROM
`maximal-furnace-783.moj_analytics.home_opened`
where
time>='2022-07-25' AND time<="2022-07-31"
group by 1
order by 1
