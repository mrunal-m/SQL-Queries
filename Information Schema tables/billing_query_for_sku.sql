  -- Costs take a few hours to show up in your BigQuery export,and might take longer than 24 hours.
  -- To send feedback about this query,click Help, and select Send feedback.
SELECT
  DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS `Day`,
  sku.description AS `SKU Description`,
  service.description AS `Service Description`,
  sku.id AS `SKU ID`,
  SUM(usage.amount_in_pricing_units) AS `Usage in Pricing Units`,
  ANY_VALUE(usage.pricing_unit) AS `Usage Pricing Unit`,
  SUM(CAST(cost AS NUMERIC)) AS `Cost`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) AS `Discounts`,
  SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `Promotions and others`,
  SUM(CAST(cost AS NUMERIC)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('SUSTAINED_USAGE_DISCOUNT',
          'DISCOUNT',
          'SPENDING_BASED_DISCOUNT',
          'COMMITTED_USAGE_DISCOUNT',
          'FREE_TIER',
          'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
          'SUBSCRIPTION_BENEFIT',
          'RESELLER_MARGIN',
          'FEE_UTILIZATION_OFFSET')), 0)) + SUM(IFNULL((
      SELECT
        SUM(CAST(c.amount AS numeric))
      FROM
        UNNEST(credits) c
      WHERE
        c.type IN ('CREDIT_TYPE_UNSPECIFIED',
          'PROMOTION')), 0)) AS `Subtotal`
FROM
  `infrastructure-platform-320405.sharechat_detailed_billing.gcp_billing_export_resource_v1_015E7D_9BBFF6_B1E1D1`
WHERE
  cost_type != 'tax'
  AND cost_type != 'adjustment'
  AND usage_start_time >= '2024-07-01T00:00:00 US/Pacific'
  AND usage_start_time < '2024-10-08T00:00:00 US/Pacific'
  AND sku.id IN ('5DE7-AD37-6FAB')
GROUP BY
  Day,
  sku.description,
  service.description,
  sku.id
ORDER BY
  Day DESC,
  Subtotal DESC
