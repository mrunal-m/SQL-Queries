SELECT *, TIMESTAMP_MILLIS(ntp_eventRecordTime_ms) ntpRecordTime FROM `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2`
WHERE callId = '9eda9ac5-e46b-4a63-aae3-078d3b6d4896'
ORDER BY 1
