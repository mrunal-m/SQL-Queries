SELECT date(creation_time, "Asia/Kolkata") as dt, --creation_time, 
SUM(total_slot_ms)/(1000*60*60*24) as slot_day from 
sc-bigquery-eppo-integration.`region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE job_id IN (
SELECT job_id from moj-prod.airflow_monitoring.dag_to_bq_job_data
WHERE dag_id IN ('cube_66a38aca-7981-452b-a331-f6a066265751','rerun_cube_66a38aca-7981-452b-a331-f6a066265751',
'cube_9caa57f6-dfad-4f3e-ac0c-b3a96f5e36dc','rerun_cube_9caa57f6-dfad-4f3e-ac0c-b3a96f5e36dc',
'cube_19740f37-2a91-4902-b5cc-75bb1c243b7f','rerun_cube_19740f37-2a91-4902-b5cc-75bb1c243b7f',
'cube_0ba2b16a-7dcf-42b9-be20-20054a4c2ad8','rerun_cube_0ba2b16a-7dcf-42b9-be20-20054a4c2ad8',
'cube_e9e3798a-23dc-4d95-bca4-ffb6424f62ac','rerun_cube_e9e3798a-23dc-4d95-bca4-ffb6424f62ac',
'cube_7e0786f8-a8f2-4b71-9fca-c22000247783','rerun_cube_7e0786f8-a8f2-4b71-9fca-c22000247783',
'cube_c16c07c0-c743-4e75-85ed-a90bd52e1abf','rerun_cube_c16c07c0-c743-4e75-85ed-a90bd52e1abf',
'cube_089ccf8d-003a-4ebf-b788-12a29c39576e','rerun_cube_089ccf8d-003a-4ebf-b788-12a29c39576e',
'cube_375c64af-beb6-4f73-94fb-7c8d27993f56','rerun_cube_375c64af-beb6-4f73-94fb-7c8d27993f56',
'cube_12c579d9-38fd-42d4-8853-f5bcba2f21db','rerun_cube_12c579d9-38fd-42d4-8853-f5bcba2f21db',
'cube_c3a2eda2-6eb4-4b9d-8d36-50c56f8e62d3','rerun_cube_c3a2eda2-6eb4-4b9d-8d36-50c56f8e62d3',
'cube_57b709d1-9c94-4735-9529-d99597e62bbd','rerun_cube_57b709d1-9c94-4735-9529-d99597e62bbd',
'cube_9432ea36-0acb-4e22-aec0-6c39b7bdb8e8','rerun_cube_9432ea36-0acb-4e22-aec0-6c39b7bdb8e8',
'cube_fd3472e1-16d4-4efe-9bf1-539569360666','rerun_cube_fd3472e1-16d4-4efe-9bf1-539569360666',
'cube_8d38e593-19ec-4b9b-92c4-26d7a123c2e4','rerun_cube_8d38e593-19ec-4b9b-92c4-26d7a123c2e4',
'cube_c9cb6cc4-881f-4ce1-bb19-595b9292ace6','rerun_cube_c9cb6cc4-881f-4ce1-bb19-595b9292ace6',
'cube_e11456d8-981c-478c-ba65-5bd1c302aa0e','rerun_cube_e11456d8-981c-478c-ba65-5bd1c302aa0e',
'cube_f4a8f4ad-058b-4c53-b18e-99cd91ede913','rerun_cube_f4a8f4ad-058b-4c53-b18e-99cd91ede913',
'cube_49b4164b-c203-4e7a-9e9a-e02671f6e259','rerun_cube_49b4164b-c203-4e7a-9e9a-e02671f6e259',
'cube_9a88bf4e-1c3b-4d0d-a25f-e6d93b55db20','rerun_cube_9a88bf4e-1c3b-4d0d-a25f-e6d93b55db20',
'cube_613c417d-cb2e-4178-a5af-1dfb7097bc95','rerun_cube_613c417d-cb2e-4178-a5af-1dfb7097bc95',
'cube_4a323f6c-45d6-4ede-abc5-6e8260fcb05f','rerun_cube_4a323f6c-45d6-4ede-abc5-6e8260fcb05f',
'cube_0bf77cc1-8a6d-41b8-ad1b-d305f253a316','rerun_cube_0bf77cc1-8a6d-41b8-ad1b-d305f253a316',
'cube_09513b2c-c38c-4c29-9dfb-c48482516e81','rerun_cube_09513b2c-c38c-4c29-9dfb-c48482516e81',
'cube_6324c5da-c275-435f-b844-bd8615c3184f','rerun_cube_6324c5da-c275-435f-b844-bd8615c3184f',
'cube_8cc8b3d6-eb20-47ce-8d01-9d85d98eebb9','rerun_cube_8cc8b3d6-eb20-47ce-8d01-9d85d98eebb9',
'cube_4cf3d5c8-9c00-40f3-96fd-99f8e961a9fc','rerun_cube_4cf3d5c8-9c00-40f3-96fd-99f8e961a9fc',
'cube_e9bc3b94-1373-49fb-8c64-b2294f136ef3','rerun_cube_e9bc3b94-1373-49fb-8c64-b2294f136ef3',
'cube_0064c40f-e7bd-45cf-b229-40b816b1a601','rerun_cube_0064c40f-e7bd-45cf-b229-40b816b1a601',
'cube_5928c9b2-e8cd-4a31-a220-04cc44f30ca4', 'cube_8965c826-4874-470e-9b02-a9ad4b6fad52',
'rerun_cube_5928c9b2-e8cd-4a31-a220-04cc44f30ca4', 'rerun_cube_8965c826-4874-470e-9b02-a9ad4b6fad52'))
AND date(creation_time) BETWEEN "2023-11-27" AND "2023-11-29"
group by 1
order by 1, 2 desc
