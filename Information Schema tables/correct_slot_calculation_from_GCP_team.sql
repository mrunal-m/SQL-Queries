CREATE TEMP FUNCTION GetSlotSecondsBetweenChanges(
  slots FLOAT64,
  range_begin_timestamp_unix_millis FLOAT64,
  range_end_timestamp_unix_millis FLOAT64,
  script_start_timestamp_unix_millis FLOAT64,
  script_end_timestamp_unix_millis FLOAT64)
RETURNS INT64
LANGUAGE js
AS r'''
    if (script_end_timestamp_unix_millis < range_begin_timestamp_unix_millis || script_start_timestamp_unix_millis > range_end_timestamp_unix_millis) {
      return 0;
    }
    var begin = Math.max(script_start_timestamp_unix_millis, range_begin_timestamp_unix_millis)
    var end = Math.min(script_end_timestamp_unix_millis, range_end_timestamp_unix_millis)
    return slots * Math.ceil((end - begin) / 1000.0)
''';

WITH
  reservation_slot_data AS (
    SELECT
      change_timestamp,
      reservation_name,
      CASE action
        WHEN 'CREATE' THEN autoscale.current_slots
        WHEN 'UPDATE'
          THEN
            IFNULL(
              autoscale.current_slots - LAG(autoscale.current_slots)
                OVER (
                  PARTITION BY project_id, reservation_name
                  ORDER BY change_timestamp ASC, action ASC
                ),
              IFNULL(
                autoscale.current_slots,
                IFNULL(
                  -1 * LAG(autoscale.current_slots)
                    OVER (
                      PARTITION BY project_id, reservation_name
                      ORDER BY change_timestamp ASC, action ASC
                    ),
                  0)))
        WHEN 'DELETE'
          THEN
            IF(
              LAG(action)
                OVER (
                  PARTITION BY project_id, reservation_name
                  ORDER BY change_timestamp ASC, action ASC
                )
                IN UNNEST(['CREATE', 'UPDATE']),
              -1 * autoscale.current_slots,
              0)
        END
        AS autoscale_current_slot_delta,
      CASE action
        WHEN 'CREATE' THEN slot_capacity
        WHEN 'UPDATE'
          THEN
            IFNULL(
              slot_capacity - LAG(slot_capacity)
                OVER (
                  PARTITION BY project_id, reservation_name
                  ORDER BY change_timestamp ASC, action ASC
                ),
              IFNULL(
                slot_capacity,
                IFNULL(
                  -1 * LAG(slot_capacity)
                    OVER (
                      PARTITION BY project_id, reservation_name
                      ORDER BY change_timestamp ASC, action ASC
                    ),
                  0)))
        WHEN 'DELETE'
          THEN
            IF(
              LAG(action)
                OVER (
                  PARTITION BY project_id, reservation_name
                  ORDER BY change_timestamp ASC, action ASC
                )
                IN UNNEST(['CREATE', 'UPDATE']),
              -1 * slot_capacity,
              0)
        END
        AS baseline_slot_delta,
    FROM
      `maximal-furnace-783`.`region-us.INFORMATION_SCHEMA.RESERVATION_CHANGES`
    WHERE
      edition = 'STANDARD'
      AND change_timestamp <= '2024-08-28 00:00:00-07'
  ),
  running_reservation_slot_data AS (
    SELECT
      change_timestamp,
      SUM(autoscale_current_slot_delta)
        OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS autoscale_current_slots,
      SUM(baseline_slot_delta)
        OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS baseline_slots,
    FROM
      reservation_slot_data
  ),
  capacity_commitment_slot_data AS (
    SELECT
      change_timestamp,
      capacity_commitment_id,
      CASE
        WHEN action = 'CREATE' OR action = 'UPDATE'
          THEN
            IFNULL(
              IF(
                LAG(action)
                  OVER (
                    PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC
                  )
                  IN UNNEST(['CREATE', 'UPDATE']),
                slot_count - LAG(slot_count)
                  OVER (
                    PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC
                  ),
                slot_count),
              slot_count)
        ELSE
          IF(
            LAG(action)
              OVER (PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC)
              IN UNNEST(['CREATE', 'UPDATE']),
            -1 * slot_count,
            0)
        END
        AS slot_count_delta
    FROM
      `maximal-furnace-783`.`region-us.INFORMATION_SCHEMA.CAPACITY_COMMITMENT_CHANGES_BY_PROJECT`
    WHERE
      state = 'ACTIVE'
      AND edition = 'STANDARD'
      AND change_timestamp <= '2024-08-28 00:00:00-07'
  ),
  running_capacity_commitment_slot_data AS (
    SELECT
      change_timestamp,
      SUM(slot_count_delta)
        OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS capacity_slot
    FROM
      capacity_commitment_slot_data
  ),
  running_capacity_commitment_slot_data_with_next_change AS (
    SELECT
      change_timestamp,
      IFNULL(LEAD(change_timestamp) OVER (ORDER BY change_timestamp ASC), CURRENT_TIMESTAMP())
        AS next_change_timestamp,
      capacity_slot
    FROM
      running_capacity_commitment_slot_data
  ),
  merged_timestamp AS (
    SELECT
      change_timestamp
    FROM
      running_reservation_slot_data
    UNION DISTINCT
    SELECT
      change_timestamp
    FROM
      running_capacity_commitment_slot_data
  ),
  running_reservation_slot_data_with_merged_timestamp AS (
    SELECT
      change_timestamp,
      IFNULL(
        autoscale_current_slots,
        IFNULL(
          LAST_VALUE(autoscale_current_slots IGNORE NULLS) OVER (ORDER BY change_timestamp ASC), 0))
        AS autoscale_current_slots,
      IFNULL(
        baseline_slots,
        IFNULL(LAST_VALUE(baseline_slots IGNORE NULLS) OVER (ORDER BY change_timestamp ASC), 0))
        AS baseline_slots
    FROM
      running_reservation_slot_data
    RIGHT JOIN
      merged_timestamp
      USING (change_timestamp)
  ),
  scaled_slots_and_baseline_not_covered_by_commitments AS (
    SELECT
      r.change_timestamp,
      IFNULL(LEAD(r.change_timestamp) OVER (ORDER BY r.change_timestamp ASC), CURRENT_TIMESTAMP())
        AS next_change_timestamp,
      r.autoscale_current_slots,
      IF(
        r.baseline_slots - IFNULL(c.capacity_slot, 0) > 0,
        r.baseline_slots - IFNULL(c.capacity_slot, 0),
        0) AS baseline_not_covered_by_commitment
    FROM
      running_reservation_slot_data_with_merged_timestamp r
    LEFT JOIN
      running_capacity_commitment_slot_data_with_next_change c
      ON
        r.change_timestamp >= c.change_timestamp
        AND r.change_timestamp < c.next_change_timestamp
  ),
  slot_seconds_data AS (
    SELECT
      change_timestamp,
      GetSlotSecondsBetweenChanges(
        autoscale_current_slots + baseline_not_covered_by_commitment,
        UNIX_MILLIS(change_timestamp),
        UNIX_MILLIS(next_change_timestamp),
        UNIX_MILLIS('2024-08-15 00:00:00-07'),
        UNIX_MILLIS('2024-08-28 00:00:00-07')) AS slot_seconds
    FROM
      scaled_slots_and_baseline_not_covered_by_commitments
    WHERE
      change_timestamp <= '2024-08-28 00:00:00-07' AND next_change_timestamp > '2024-08-15 00:00:00-07'
  )
SELECT
  SUM(slot_seconds)/3600 AS total_slot_hours
FROM
  slot_seconds_data
