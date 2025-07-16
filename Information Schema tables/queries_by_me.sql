SELECT creation_time, job_id, user_email, query FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT 
WHERE date(creation_time) = "2025-07-03"
  AND user_email = 'mrunalmendgudle@sharechat.co'
