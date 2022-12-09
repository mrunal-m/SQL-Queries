--moj-prod timeout query error info schema info_schema information_schema
(with t1 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-20") AND error_result.reason LIKE "%timeout%"
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com" ),

t2 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-20") AND error_result.reason IS NULL
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com"  )

SELECT t1.query, t1.creation_time as fail_creation_time, t1.duration as fail_duration,
t2.creation_time as retry_creation_time, t2.duration as retry_duration
from t1 INNER JOIN t2 ON t1.query=t2.query
WHERE t2.creation_time > t1.creation_time )

UNION ALL

(with t1 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-22") AND error_result.reason LIKE "%timeout%"
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com" ),

t2 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-22") AND error_result.reason IS NULL
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com"  )

SELECT t1.query, t1.creation_time as fail_creation_time, t1.duration as fail_duration,
t2.creation_time as retry_creation_time, t2.duration as retry_duration
from t1 INNER JOIN t2 ON t1.query=t2.query
WHERE t2.creation_time > t1.creation_time )

UNION ALL

(with t1 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-24") AND error_result.reason LIKE "%timeout%"
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com" ),

t2 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-24") AND error_result.reason IS NULL
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com"  )

SELECT t1.query, t1.creation_time as fail_creation_time, t1.duration as fail_duration,
t2.creation_time as retry_creation_time, t2.duration as retry_duration
from t1 INNER JOIN t2 ON t1.query=t2.query
WHERE t2.creation_time > t1.creation_time )

UNION ALL

(with t1 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-26") AND error_result.reason LIKE "%timeout%"
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com" ),

t2 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-26") AND error_result.reason IS NULL
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com"  )

SELECT t1.query, t1.creation_time as fail_creation_time, t1.duration as fail_duration,
t2.creation_time as retry_creation_time, t2.duration as retry_duration
from t1 INNER JOIN t2 ON t1.query=t2.query
WHERE t2.creation_time > t1.creation_time )

UNION ALL

(with t1 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-28") AND error_result.reason LIKE "%timeout%"
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com" ),

t2 as (SELECT creation_time, query, start_time, end_time, end_time - start_time as duration
from `moj-prod.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE (date(creation_time) = "2022-11-28") AND error_result.reason IS NULL
AND user_email = "ds-moj-composer-sa@moj-prod.iam.gserviceaccount.com"  )

SELECT t1.query, t1.creation_time as fail_creation_time, t1.duration as fail_duration,
t2.creation_time as retry_creation_time, t2.duration as retry_duration
from t1 INNER JOIN t2 ON t1.query=t2.query
WHERE t2.creation_time > t1.creation_time )

ORDER BY 2 ASC
