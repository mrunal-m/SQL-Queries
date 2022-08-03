SELECT
DATE(time),
count(*) as total_count,
count(case when videoStartTime is null then 1 end) as videoStartTime,
count(case when timeSpent is null then 1 end) as timeSpent,
count(case when duration is null then 1 end) as duration,
count(case when sessionID is null then 1 end) as sessionID,
count(case when userId is null then 1 end) as userId,
count(case when percentageFloat is null then 1 end) as percentageFloat,
count(case when videoSessionId is null then 1 end) as videoSessionId,
count(case when ntp_eventRecordTime is null then 1 end) as ntp_eventRecordTime,
count(case when ntp_eventDispatchTime is null then 1 end) as ntp_eventDispatchTime,
count(case when tagId is null then 1 end) as tagID,
count(case when tagname is null then 1 end) as tagName,
count(case when videoPlayType is null then 1 end) as videoPlayType,
count(case when eventTime is null then 1 end) as eventTime,
count(case when totalPlayTime is null then 1 end) as totalPlayTime,
count(case when videoType is null then 1 end) as videoType,
count(case when adTime is null then 1 end) as adTime,
count(case when adCount is null then 1 end) as adCount,
count(case when start_time_delay_after_ad is null then 1 end) as start_time_delay_after_ad,

FROM
`maximal-furnace-783.sc_analytics.video_play`
where
date(time)>="2022-07-25" AND date(time)<="2022-07-31"
group by 1
order by 1
