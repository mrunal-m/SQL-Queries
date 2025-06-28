WITH 
exp_base AS (
SELECT DISTINCT date(TIMESTAMP,'Asia/Kolkata') as dt, variant, userId,
from `sharechat-production.experimentationV2.abTest_view_events_backendV2` 
where expID = "88dede7d-8f78-4d19-b5b3-6c115a6737d1"
and DATE(TIMESTAMP,"Asia/Kolkata") >= "2025-06-26"
and timestamp(timestamp)>=TIMESTAMP('2025-06-26 12:30:00 UTC')
and version not in ('NA') and version > '0'
),

v2_base AS (
SELECT DATE(time,"Asia/Kolkata") dt, packcalltime,  packCategory, userId, rechargeTransactionId, cast(packAmount as int) AS cost
FROM `maximal-furnace-783.vibely_analytics.consultation_recharge`
 WHERE status = 'accepted' AND referralId = 'InCallRecharge' AND DATE(time,"Asia/Kolkata") >= CURRENT_DATE("Asia/Kolkata") - 2
 GROUP BY ALL),

agg AS (
SELECT v2_base.dt, exp_base.variant, v2_base.userId, packCategory mediaType, packcalltime, rechargeTransactionId, cost 
FROM exp_base INNER JOIN v2_base ON 
exp_base.dt = v2_base.dt AND 
exp_base.userId = v2_base.userId
)
SELECT dt, variant, mediaType, packcalltime, COUNT(DISTINCT userId) countUsers,
COUNT(DISTINCT rechargeTransactionId) transactions, SUM(cost) rechagreGMV FROM agg
GROUP BY ALL
ORDER BY 1, 2, 3, 4
