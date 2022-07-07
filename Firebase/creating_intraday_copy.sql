--Creating Intraday Copy
--It is accurate - checked with intraday table 

CREATE TABLE sc-bigquery-product-analyst.firebase_intraday_testing.events_intraday_20220706
AS (SELECT * from sharechat-firebase.analytics_163194662.events_intraday_20220706)
