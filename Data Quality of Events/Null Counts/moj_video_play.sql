SELECT
DATE(time),
count(*) as total_count,
count(case when name is null then 1 end) as name,
count(case when meta is null then 1 end) as meta,
count(case when mode is null then 1 end) as mode,
count(case when videoStartTime is null then 1 end) as videoStartTime,
count(case when timeSpent is null then 1 end) as timeSpent,
count(case when duration is null then 1 end) as duration,
count(case when sessionID is null then 1 end) as sessionID,
count(case when userId is null then 1 end) as userId,
count(case when percentageFloat is null then 1 end) as percentageFloat,
count(case when ntp_eventRecordTime is null then 1 end) as ntp_eventRecordTime,
count(case when ntp_eventDispatchTime is null then 1 end) as ntp_eventDispatchTime,
count(case when tagId is null then 1 end) as tagID,
count(case when tagname is null then 1 end) as tagName,
count(case when referrer is null then 1 end) as referrer,
count(case when audioVolume is null then 1 end) as audioVolume,
count(case when appVersion is null then 1 end) as appVersion,
count(case when clientType is null then 1 end) as clientType,
count(case when videoUrl is null then 1 end) as videoUrl,

FROM
`maximal-furnace-783.moj_analytics.video_play`
where
date(time)>='2022-07-25' AND date(time)<="2022-07-31"
group by 1
order by 1
