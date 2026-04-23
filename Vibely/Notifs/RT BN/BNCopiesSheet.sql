-- sheet https://docs.google.com/spreadsheets/d/1h13Rc9bZLScryTdjx4khjnfQz5VrGRIWlvi_yW05qJg/edit?gid=1226323505#gid=1226323505
TRUNCATE TABLE maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig;
INSERT INTO maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig
(
SELECT *, '' fsIconImage, 0	timerInMins FROM maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfigSheet
)
