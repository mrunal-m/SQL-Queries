-- task 4 IconsOnboardingCohorts.sql
-- new query for d0, d1, d7, d21 logic
-- CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.IconsOnboardingCohorts` AS 
(
WITH user_past_data AS ( -- Precompute all relevant past user appearances in a single scan
    SELECT userId,
        COUNTIF(date(date) = current_date() - 1) AS in_d1,
        COUNTIF(date(date) = current_date() - 2) AS in_d2,
        COUNTIF(date(date) = current_date() - 7) AS in_d7,
        COUNTIF(date(date) = current_date() - 21) AS in_d21,
        COUNTIF(date(date) < current_date()) AS was_ever_in_past
    FROM maximal-furnace-783.vibely_analytics.crmDataWithTemplate
    WHERE date(date) <= current_date() AND cohort IN ('IconsOnboarding', 'IconsAppUpdate')
    GROUP BY userId),

new_users_today AS ( -- Users appearing for the first time today in icons enabled cohort
    SELECT DISTINCT userId FROM `maximal-furnace-783.vibely_analytics.vibely_icon_enabled_cohort` -- change table
    WHERE date(dt) = current_date("Asia/Kolkata")
    AND userID NOT IN (SELECT userId from maximal-furnace-783.vibely_analytics.crmDataWithTemplate
    WHERE date(date) <= current_date() AND cohort IN ('IconsOnboarding', 'IconsAppUpdate') ) ),

eligible_users AS ( -- Apply filtering logic using precomputed fields
    SELECT userId FROM user_past_data
    WHERE -- Include users who were in D-1 only (but not D-2)
        (in_d1 = 1 AND in_d2 = 0)  OR in_d7 = 1 OR in_d21 = 1
        -- Include completely new users who have never appeared before
        OR was_ever_in_past = 0
    UNION ALL  -- Include new users from `vibely_icon_enabled_cohort`
    SELECT userId FROM new_users_today),

t1 AS ( -- Video onboarding and user details
    WITH icons_cohort AS (SELECT * FROM `maximal-furnace-783.vibely_analytics.vibely_icon_enabled_cohort`),

    home_opened AS (SELECT CAST(distinct_id AS STRING) AS userId, MAX(appVersion) AS app_version
        FROM `maximal-furnace-783.vibely_analytics.home_opened`
        WHERE date(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 21
        AND tenant = 'fz' GROUP BY userId ),

    calls AS (SELECT * FROM ( SELECT a.*, TIMESTAMP_DIFF(TIMESTAMP(session_ended_at),TIMESTAMP(session_started_at), SECOND) / 60.0 AS total_call_time,    DATE(time, 'Asia/Kolkata') AS dt, ROW_NUMBER() OVER (PARTITION BY a.consultation_id, vendor_session_id, status ORDER BY rowIngestionTime) AS rn
            FROM `maximal-furnace-783.sc_analytics.consultation` a
            WHERE date(time, 'Asia/Kolkata') >= current_date('Asia/Kolkata') - 30 
            AND consultation_type = 'icons' --update consultation type
            AND tenant = "fz") WHERE rn = 1 AND status = 'completed'-- removed mediatype = video
            ),

    userPhoneNo AS (SELECT DISTINCT Id, phoneNo, name FROM maximal-furnace-783.sc_analytics.user
        WHERE tenant = 'fz'),

    no_icons_caller AS ( SELECT a.* FROM icons_cohort a
        LEFT JOIN calls b ON a.userId = b.consultee_user_id
        WHERE b.consultee_user_id IS NULL)

    SELECT  a.*, 
        CASE WHEN b.userId IS NULL THEN 0 ELSE 1 END AS home_opened, phoneNo,  name AS userName,
        CASE WHEN type='user' and dt>='2025-03-20' and app_version >= 202500601 THEN 1 --change icons build
            WHEN app_version < 202500601 THEN 0 
            ELSE -1  END AS icons_build
    FROM no_icons_caller a
    LEFT JOIN home_opened b ON a.userId = b.userId
    LEFT JOIN userPhoneNo c ON a.userId = c.Id 
),

t2 AS (SELECT current_date("Asia/Kolkata") AS date,  CASE 
            WHEN t1.home_opened = 1 AND t1.icons_build = 0 THEN 'IconsAppUpdate' 
            WHEN t1.home_opened = 1 AND t1.icons_build = 1 THEN 'IconsOnboarding'
            ELSE NULL END AS cohort,  language, userId, phoneNo, userName 
    FROM t1  WHERE icons_build NOT IN (-1)
    AND userId IN (SELECT userId FROM eligible_users)
)
(SELECT t2.* from t2)

)

-- Changes appV, conultation_type, icons_enabled_cohort table
