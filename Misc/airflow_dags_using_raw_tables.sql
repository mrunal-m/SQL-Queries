with t1 AS (SELECT job_id, case when (referenced_tables.project_id = 'maximal-furnace-783'
      AND referenced_tables.dataset_id IN ('moj_analytics', 'sc_analytics')) then 1
      else 0 end raw_table_indicator
FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION i,
    UNNEST(referenced_tables) referenced_tables
    WHERE date(creation_time) = "2024-08-05"
    AND job_id IN (
SELECT job_id from maximal-furnace-783.data_platform_temp1.airflow_slot_usage
WHERE DAG_id IN ('PROD_sc_ranker_base_hourly_logging_v22_dataset__v4',
'user_demographic_daily_schedule_tasks',
'sc_post_15min_preagg_v2',
'ads-campaign-metric-percentile-scylla_bq_to_db',
'PROD_sc_ranker_base_hourly_logging_v2_dataset_v0_v6',
'lfsct-prod-webhook-generation',
'sc_ads_monolith_training_data',
'PROD_sc-feed-content-journey_ffm-post-filter',
'sc_trigger-based-counters-sanity',
'lfsct-prod-cg_notifType',
'affinity_monitoring_dag',
'sc_dq_rtffm_batch_match_staleness',
'ads_user_category_engagement_features',
'maker-checker-feature-sanity',
'user_demographic_data_preparation',
'notification-publishing-prod-ds-v-tt-sc',
'sc_post-static-features-structured-v1',
'PROD_sc-feed-content-journey_ffm-realtime-avg-emb-dump-daily',
'light_ranker_cg_incremental',
'ads-ds-monte-carlo-train-table-stats-compute',
'sc_extract-features-from-logged-data',
'PROD_sc-feed-content-journey_Post_candidates',
'PROD_sc_ranker_train_warm_video_v2_1',
'PROD_sc_ranker_train_warm_video_v2_3',
'ads-sc-promoted-posts',
'moj_ads_monolith_training_data',
'notification-publishing-prod-default_ds_control',
'PROD_sc-feed-content-journey_ffm-realtime-export-video2',
'sc_dq_features_hourly_sanity',
'user_inmarket_features_bq_to_db',
'sc_ads_monolith_validation_data',
'light_ranker_cg_extract_embeddings',
'PROD_sc-feed-content-journey_realtime-agg-hourly',
'moj_feature_health_hourly_dag',
'sc_dq_lastK_match_rate',
'ads-ds-monte-carlo-feature-pre-agg-compute-SC',
'sc_gd_ab_3hr_stable',
'sc_gd_ab_24hr_stable',
'live-notifications-v0',
'PROD_akhil_expt',
'hamsa-minview-inserts-v2',
'PROD_sc-feed-content-journey_realtime-ffm-calib-model',
'notification-publishing-prod-ddc_exclude_users_cgpopular_ho',
'sc_ads_yield_dashboard',
'user_counters_1_day_gaid_scylla_bq_to_db',
'notif-feature-logging-prod-sc',
'monolith_ranker_performance_tracking_v1',
'lfsct-prod-user_genre_preagg',
'ffm-realtime64-dump',
'ads-promoted-posts-mj-64-realtime-embeddings')
AND dt = "2024-08-05"
    )),

t2 AS (select dag_id, job_id, SUM(slot_day) slot_day from maximal-furnace-783.data_platform_temp1.airflow_slot_usage
group by all)

SELECT t2.dag_id, max(raw_table_indicator) raw_table_indicator, SUM(t2.slot_day) slot_day from t1 LEFT JOIN t2 ON
t1.job_id = t2.job_id
group by all
order by 1 , 3 desc
-- group by all
-- order by 1 desc
