WITH 

-- 1️⃣ Experiment exposure (user-level, first exposure)
exp_base AS (
  SELECT
    variant,
    userId,
    MIN(DATE(timestamp,'Asia/Kolkata')) AS dt,
    MIN(DATETIME(timestamp,'Asia/Kolkata')) AS dt_time,
    MIN(timestamp) AS timeMin
  FROM `sharechat-production.experimentationV2.abTest_view_events_backendV2`
  WHERE expID = "a6ea6e08-df1d-409b-9817-225d75765a4f"
    AND DATE(timestamp,'Asia/Kolkata') >= '2026-01-23'
    AND version NOT IN ('NA')
    AND version > '0'
  GROUP BY 1,2
),

-- 2️⃣ Call count base
count_base AS (
  SELECT
    consultation_id AS call_id,
    MAX(consultation_count) AS call_no
  FROM `maximal-furnace-783.sc_analytics.consultation`
  WHERE  DATE(time,'Asia/Kolkata') >= '2024-01-23'
  GROUP BY 1
),

-- 3️⃣ De-duplicated completed calls
calls AS (
  SELECT *
  FROM (
    SELECT
      a.consultation_id,
      a.consultee_user_id,
      CAST(cb.call_no AS INT64) AS call_no,
      TIMESTAMP_DIFF(
        TIMESTAMP(session_ended_at),
        TIMESTAMP(session_started_at),
        SECOND
      ) / 60.0 AS total_call_time,
      total_charges,
      DATETIME(time,'Asia/Kolkata') AS dt_time,
      ROW_NUMBER() OVER (
        PARTITION BY a.consultation_id, vendor_session_id, status
        ORDER BY rowIngestionTime
      ) AS rn
    FROM `maximal-furnace-783.sc_analytics.consultation` a
    JOIN count_base cb
      ON cb.call_id = a.consultation_id
    WHERE consultation_type = 'FIND_A_FRIEND'
      AND tenant = 'fz'
      AND status = 'completed'
      AND
        DATE(a.time,'Asia/Kolkata') >= '2024-01-23'
  )
  WHERE rn = 1
),

-- 4️⃣ USER-LEVEL call aggregation (post exposure) ✅
user_calls AS (
  SELECT
    e.variant,
    e.userId,

    COUNT(DISTINCT c.consultation_id) AS calls,
    SUM(c.total_call_time) AS total_call_time,
    SUM(c.total_charges)/3.5 AS gmv_rs,

    COUNT(DISTINCT CASE WHEN c.call_no > 1 THEN c.consultation_id END) AS pu_calls,
    SUM(CASE WHEN c.call_no > 1 THEN c.total_call_time END) AS pu_call_time,
    SUM(CASE WHEN c.call_no > 1 THEN c.total_charges END)/3.5 AS pu_gmv_rs

  FROM exp_base e
  LEFT JOIN calls c
    ON e.userId = c.consultee_user_id
   AND c.dt_time >= e.dt_time
  GROUP BY 1,2
),

-- 5️⃣ USER-LEVEL call bucket (pre exposure) ✅
user_call_bucket AS (
  SELECT
    c.consultee_user_id AS userId,
    CASE
      WHEN MAX(SAFE_CAST(consultation_count AS INT64)) = 1 THEN '1'
      WHEN MAX(SAFE_CAST(consultation_count AS INT64)) BETWEEN 2 AND 5 THEN '2-5'
      WHEN MAX(SAFE_CAST(consultation_count AS INT64)) BETWEEN 6 AND 10 THEN '6-10'
      WHEN MAX(SAFE_CAST(consultation_count AS INT64)) BETWEEN 11 AND 20 THEN '11-20'
      WHEN MAX(SAFE_CAST(consultation_count AS INT64)) > 20 THEN '20+'
      ELSE 'noCalls'
    END AS call_bucket
  FROM `maximal-furnace-783.sc_analytics.consultation` c
  JOIN exp_base e 
    ON e.userId = c.consultee_user_id
   AND c.time <= e.timeMin
  WHERE consultation_type = 'FIND_A_FRIEND' AND
  DATE(c.time,'Asia/Kolkata') >= '2024-01-23'
    AND tenant = 'fz'
  GROUP BY 1
),

-- 6️⃣ USER-LEVEL recharge aggregation (post exposure) ✅
recharge_completed AS (
  SELECT
    e.variant,
    e.userId,
    SUM(a.cost) AS recharge_gmv_rs,
    SUM(a.units) AS recharge_gmv_coins
  FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` a
  JOIN exp_base e
    ON a.distinct_id = e.userId
   AND DATETIME(a.time,'Asia/Kolkata') >= e.dt_time
  WHERE LOWER(a.status) = 'success'
    AND DATE(a.time,'Asia/Kolkata') >= '2026-01-23'
  GROUP BY 1,2
)

-- 7️⃣ FINAL: Variant × Call bucket (NO inflation)
SELECT
  uc.variant,
  ucb.call_bucket,

  COUNT(DISTINCT uc.userId) AS exp_users,
  COUNT(DISTINCT CASE WHEN uc.calls > 0 THEN uc.userId END) AS callers,

  SUM(uc.calls) AS calls,
  SUM(uc.total_call_time) AS total_call_time,
  SUM(uc.gmv_rs) AS gmv_rs,

  COUNT(DISTINCT CASE WHEN uc.pu_calls > 0 THEN uc.userId END) AS pu,
  SUM(uc.pu_calls) AS pu_calls,
  SUM(uc.pu_call_time) AS pu_call_time,
  SUM(uc.pu_gmv_rs) AS pu_gmv_rs,

  SUM(rc.recharge_gmv_rs) AS recharge_gmv_rs,
  SUM(rc.recharge_gmv_coins) AS recharge_gmv_coins

FROM user_calls uc
LEFT JOIN user_call_bucket ucb
  ON uc.userId = ucb.userId
LEFT JOIN recharge_completed rc
  ON uc.userId = rc.userId
 AND uc.variant = rc.variant

GROUP BY 1,2
ORDER BY 1,2;
