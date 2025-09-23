--my og query
WITH landing_user AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, SUBSTRING(userId, 2) userId, clientType, screen, action, utm_link,
REGEXP_EXTRACT(utm_link, r'[?&]utm_campaign=([^&]+)') AS campaign,
FROM `maximal-furnace-783.vibely_analytics.vibely_onboarding` 
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) 
AND screen = 'trial' AND action = 'webp_view' AND clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz'),

callCTA AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, SUBSTRING(userId, 2) userId, clientType, screen, action,
FROM `maximal-furnace-783.vibely_analytics.vibely_onboarding` 
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) 
AND action = 'call_CTA_clicked' AND clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz'),

number_verify AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, userId,  action, clientType --distinct_id, userId, clientType, action
FROM `maximal-furnace-783.vibely_analytics.number_verify_activity`
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC'))
AND clientType IN ('mweb-android', 'mweb-ios') ),

mobile_entered AS (SELECT * FROM number_verify WHERE action = 'mobile_number_entered'),
get_otp AS (SELECT * FROM number_verify WHERE action = 'get_otp_CTA_clicked'),
otp_screen AS (SELECT * FROM number_verify WHERE action = 'otp_screen_view'),
submit_otp_CTA AS (SELECT * FROM number_verify WHERE action = 'submit_otp_CTA_clicked'),

otp_sentBE AS (SELECT DATE(time, "Asia/Kolkata") dt, userId, clientType 
FROM `maximal-furnace-783.vibely_analytics.otp_request_event`
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND otpSent IS TRUE
AND clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz'),

profile_creation AS (SELECT DATE(time, "Asia/Kolkata") dt, SUBSTRING(userId, 2) userId, clientType, action 
FROM `maximal-furnace-783.vibely_analytics.profile_creation_events`
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) 
AND clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz'),

nameView AS (SELECT * FROM profile_creation WHERE action = 'name_screen_view'),
genderView AS (SELECT * FROM profile_creation WHERE action = 'gender_screen_view'),


acc_verification AS (SELECT DATE(time, "Asia/Kolkata") dt, 
CASE WHEN loginType = 'newSignUp' THEN distinct_id
WHEN loginType = 'relogin' THEN verifiedUserId 
ELSE distinct_id END userId, status, clientType,	loginType
FROM `maximal-furnace-783.vibely_analytics.account_verification`
WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND status = 'success'
AND clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz' ),

paymentInit AS (SELECT DATE(time, "Asia/Kolkata") dt, SAFE_CAST(distinct_id AS STRING) as userId 
    FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event`
    WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND status = 'INITIATED'),

paymentSuccess AS (SELECT DATE(time, "Asia/Kolkata") dt, SAFE_CAST(distinct_id AS STRING) as userId 
    FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event`
    WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND status = 'SUCCESS'),

paymentFE AS (SELECT DISTINCT DATE(_eventRecordTime, "Asia/Kolkata") dt, SAFE_CAST(_eventMeta.userProperties.userId AS STRING) as userId, action
FROM `maximal-furnace-783.vibely_analytics.post_payment_activity_fe`
WHERE DATE(_eventRecordTime, "Asia/Kolkata") >= "2025-09-23"),

paymentPageView AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, SUBSTRING(userId, 2) userId, 
FROM `maximal-furnace-783.vibely_analytics.sc_open_customized_payments_page`
WHERE  status = 'payment_page_landed' AND (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND
clientType IN ('mweb-android', 'mweb-ios') AND tenant = 'fz'),

paymentSuccessView AS (SELECT * FROM paymentFE WHERE action = 'payment_success_screen_view'),
paymentFailureView AS (SELECT * FROM paymentFE WHERE action = 'payment_failed_screen_view'),
paymentProcessingView AS (SELECT * FROM paymentFE WHERE action = 'payment_processing_screen_view'),
CheckStatusView AS (SELECT * FROM paymentFE WHERE action = 'check_status_clicked'),
DownloadAppView AS (SELECT * FROM paymentFE WHERE action = 'download_app_screen_view'),
AutoRedirect AS (SELECT * FROM paymentFE WHERE action IN ('auto_redirect_to_playstore', 'auto_redirect_to_appstore')),
DownloadAppCTAClick AS (SELECT * FROM paymentFE WHERE action = 'download_app_CTA_clicked'),

ho AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, SAFE_CAST(distinct_id AS STRING) userId
    FROM `maximal-furnace-783.vibely_analytics.home_opened`
    WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND tenant = 'fz'),

Caller AS (SELECT DISTINCT DATE(time, "Asia/Kolkata") dt, SAFE_CAST(consultee_user_id AS STRING) as userId
    FROM `maximal-furnace-783.sc_analytics.consultation`
    WHERE (DATE(time, "Asia/Kolkata") >= "2025-09-23" AND TIMESTAMP(time) >= TIMESTAMP('2025-09-22 10:00:00 UTC')) AND  status = 'completed')


-- SELECT * FROM DownloadAppCTAClick

SELECT landing_user.clientType, landing_user.campaign,
-- acc_verification.loginType,
COUNT(DISTINCT landing_user.userId) webpView,
COUNT(DISTINCT callCTA.userId) callCTACLick,
COUNT(DISTINCT mobile_entered.userId) mobile_entered,
COUNT(DISTINCT get_otp.userId) get_otp_CTA,
COUNT(DISTINCT otp_screen.userId) otp_screen_View,
COUNT(DISTINCT submit_otp_CTA.userId) submit_otp_CTA,
COUNT(DISTINCT otp_sentBE.userId) otp_sentBE,
COUNT(DISTINCT acc_verification.userId) acc_verification,

COUNT(DISTINCT nameView.userId) nameView,
COUNT(DISTINCT genderView.userId) genderView,

COUNT(DISTINCT paymentPageView.userId) paymentPageView,
COUNT(DISTINCT paymentSuccessView.userId) paymentSuccessView,
COUNT(DISTINCT paymentFailureView.userId) paymentFailureView,

COUNT(DISTINCT paymentInit.userId) paymentInit,
COUNT(DISTINCT paymentSuccess.userId) paymentSuccess,

COUNT(DISTINCT DownloadAppView.userId) DownloadAppView,
COUNT(DISTINCT AutoRedirect.userId) AutoRedirect,
COUNT(DISTINCT DownloadAppCTAClick.userId) DownloadAppCTAClick,

COUNT(DISTINCT ho.userId) HO,


FROM landing_user 
LEFT JOIN callCTA ON landing_user.userId = callCTA.userId
LEFT JOIN mobile_entered ON callCTA.userId = mobile_entered.userId
LEFT JOIN get_otp ON mobile_entered.userId = get_otp.userId
LEFT JOIN otp_screen ON mobile_entered.userId = otp_screen.userId
LEFT JOIN submit_otp_CTA ON mobile_entered.userId = submit_otp_CTA.userId
LEFT JOIN otp_sentBE ON mobile_entered.userId = otp_sentBE.userId
LEFT JOIN acc_verification ON submit_otp_CTA.userId = acc_verification.userId

LEFT JOIN nameView ON acc_verification.userId = nameView.userId
LEFT JOIN genderView ON acc_verification.userId = genderView.userId

LEFT JOIN paymentPageView ON genderView.userId = paymentPageView.userId
LEFT JOIN paymentSuccessView ON paymentPageView.userId = paymentSuccessView.userId
LEFT JOIN paymentFailureView ON paymentPageView.userId = paymentFailureView.userId

LEFT JOIN paymentInit ON submit_otp_CTA.userId = paymentInit.userId
LEFT JOIN paymentSuccess ON paymentInit.userId = paymentSuccess.userId 
LEFT JOIN DownloadAppView ON paymentPageView.userId = DownloadAppView.userId
LEFT JOIN AutoRedirect ON paymentPageView.userId = AutoRedirect.userId
LEFT JOIN DownloadAppCTAClick ON paymentPageView.userId = DownloadAppCTAClick.userId

LEFT JOIN ho ON paymentPageView.userId = ho.userId

GROUP BY ALL
