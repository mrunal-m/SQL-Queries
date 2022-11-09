--ts query for dcewjhefbie
MERGE `maximal-furnace-783.data_platform_temp1.sess_hrly_copy_9_10jun22` t USING
(
with t1 as (
  select
    *
  except(event_timestamp),
    event_timestamp - event_server_timestamp_offset as event_timestamp
  from
    `sc-bigquery-product-analyst.firebase_intraday_testing.events_intraday_*`,
    UNNEST(event_params) as evt_prms
  where
    evt_prms.key = 'engagement_time_msec'
    and app_info.id in ('in.mohalla.sharechat', 'in.mohalla.video')
    and user_id is not null -- 		and parse_DATE("%Y%m%d", SUBSTR(_table_suffix, 1)) <= current_date
    and parse_DATE("%Y%m%d", SUBSTR(_table_suffix, 1)) >= date_sub(current_date, interval 14 day)
    and parse_DATE("%Y%m%d", SUBSTR(_table_suffix, 1)) <= date_sub(current_date, interval 12 day)
),
t2 as (
  select
    timestamp_millis(cast(event_timestamp / 1000 -(select evt_prms.value.int_value from UNNEST(event_params) as evt_prms where evt_prms.key = 'engagement_time_msec') 
                    as int64)) event_start_time,
    ( select user.value.string_value from UNNEST(user_properties) as user where user.key = 'android_device_id') as deviceid,
    user_id as user_id,
    (select evt_prms.value.int_value from UNNEST(event_params) as evt_prms where evt_prms.key = 'ga_session_id') as ga_session_id,
    ( select evt_prms.value.int_value from UNNEST(event_params) as evt_prms where evt_prms.key = 'ga_session_number') as ga_session_number,
    app_info.id as app_id,
    event_timestamp -1000 *(select evt_prms.value.int_value from UNNEST(event_params) as evt_prms where evt_prms.key = 'engagement_time_msec') as event_start_timestamp,
    ( select evt_prms.value.int_value from UNNEST(event_params) as evt_prms where evt_prms.key = 'engagement_time_msec') as time_spent_millis,
    event_server_timestamp_offset,
  from
  t1
)
select
  distinct *
from
  t2
where
  time_spent_millis > 0
  and date(event_start_time) >= date_sub(current_date, interval 14 day)
) s
ON t.user_id = s.user_id and t.event_start_time = s.event_start_time
WHEN NOT MATCHED BY SOURCE AND date(event_start_time)>= date_sub(CURRENT_DATE(), interval 14 day) THEN DELETE
WHEN NOT MATCHED BY TARGET THEN
insert
(event_start_time, deviceid, user_id, ga_session_id, ga_session_number, app_id, event_start_timestamp, time_spent_millis, event_server_timestamp_offset)
values
(event_start_time, deviceid, user_id, ga_session_id, ga_session_number, app_id, event_start_timestamp, time_spent_millis, event_server_timestamp_offset)
