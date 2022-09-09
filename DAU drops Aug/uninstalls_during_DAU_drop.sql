--Uninstalls
SELECT COUNT(*) as count_uninstall

FROM `sharechat-firebase.analytics_163194662.events_20220827`

WHERE
  app_info.id = 'in.mohalla.video'
  and event_name ="app_remove"
  and (time(TIMESTAMP_MICROS(SAFE_CAST(event_timestamp AS INT64)) )
  BETWEEN time(datetime "2022-08-27 05:06:00.000000") AND time(datetime"2022-08-27 05:56:00.000000"))
