FOR i IN (SELECT table_name FROM maximal-furnace-783.moj_analytics.INFORMATION_SCHEMA.TABLES
WHERE table_name LIKE "%to_delete_cdp") 
DO
  BEGIN
    -- Try executing the ALTER TABLE statement
    BEGIN
      EXECUTE IMMEDIATE FORMAT(""" DROP TABLE `maximal-furnace-783.moj_analytics.%s` """, i.table_name);
    EXCEPTION WHEN ERROR THEN
      -- Handle the error (BigQuery doesn't support this directly, so logging needs to be done externally)
      SELECT FORMAT('ERROR for: %s', i.table_name);
    END;
  END;
END FOR;
