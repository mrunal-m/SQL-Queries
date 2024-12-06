-- Replace 'your_project_id' and 'your_dataset_id' with your actual project and dataset IDs
DECLARE dataset_id STRING DEFAULT 'gam_dtr_reports';
DECLARE project_id STRING DEFAULT 'maximal-furnace-783';

BEGIN
  -- Declare variables at the beginning of the block
  DECLARE full_table_name STRING;

  -- Loop over all tables with partitioning in the specified dataset
  FOR record IN (
    SELECT DISTINCT table_name
    FROM `maximal-furnace-783`.`gam_dtr_reports`.INFORMATION_SCHEMA.PARTITIONS
  )
  DO
    -- Construct the full table name
    SET full_table_name = FORMAT('`%s.%s.%s`', project_id, dataset_id, record.table_name);

    -- Set the partition expiration to 7 days
    EXECUTE IMMEDIATE FORMAT(
      'ALTER TABLE %s SET OPTIONS ( partition_expiration_days = 7 )', full_table_name);

    -- Optional: Output confirmation
    SELECT FORMAT('Set partition expiration to 7 days on table: %s', full_table_name) AS confirmation;
  END FOR;
END;
