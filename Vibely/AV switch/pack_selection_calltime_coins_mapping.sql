with
recharge_intent as(
  select distinct DATE(time,'Asia/Kolkata') as dt,
  split(userId,'U')[1] as userId, 
  callId,
  JSON_VALUE(meta, '$.payRequestId') AS payRequestId
  from `maximal-furnace-783.vibely_analytics.private_consultation_in_app_user_events_v2`
  where DATE(time,'Asia/Kolkata')
  between current_date("Asia/Kolkata")-1 and current_date("Asia/Kolkata")
  and action ='rechargeClicked'
),

pm_clicked_with_orderId as(
  SELECT distinct
  DATE(time,'Asia/Kolkata') as dt,
  trim(userid,'U') userid,
  uuid as payRequestId,
  FROM `maximal-furnace-783.vibely_analytics.sc_recharge_wallet`
  WHERE DATE(time,"Asia/Kolkata") between current_date("Asia/Kolkata")-1 and current_date("Asia/Kolkata")
  and transactionId is not null
  and lower(referrer_component) like '%incallrecharge%'
),

recharge_completed as(
  SELECT distinct DATE(time,'Asia/Kolkata') as dt,
  userId,
  uuid as payRequestId,
  transactionId,
  row_number() over(partition by uuid order by time desc) as w
  FROM `maximal-furnace-783.vibely_analytics.virtual_gifting_recharge_confirmation_event` a
  WHERE DATE(time,"Asia/Kolkata") between current_date("Asia/Kolkata")-1 and current_date("Asia/Kolkata")
  and lower(a.status) = "success"
  qualify w=1
),

recharge_history as(
  select distinct  
  DATE(a.time,'Asia/Kolkata') as dt,
  rechargeTransactionId AS transactionId,
  cast(packAmount as int) AS cost,
  cast(packCoins as int) AS coins,
  userId,
  packCategory,
  packCallTime,
  referralId,
  clientType
  FROM `maximal-furnace-783.vibely_analytics.consultation_recharge` a 
  WHERE DATE(a.time,'Asia/Kolkata') BETWEEN CURRENT_DATE("Asia/Kolkata")-1 AND CURRENT_DATE("Asia/Kolkata")-1
  and referralId='InCallRecharge'
)

select distinct 
cost,
coins,
packCategory,
packCallTime,
from recharge_intent b 
left join pm_clicked_with_orderId f
on b.payRequestId=f.payRequestId
left join recharge_completed h
on f.payRequestId=h.payRequestId
left join recharge_history i 
on h.transactionId=i.transactionId
