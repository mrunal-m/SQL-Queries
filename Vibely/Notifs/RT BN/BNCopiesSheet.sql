TRUNCATE TABLE maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig;
INSERT INTO maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig
(
SELECT *, '' fsIconImage, 0	timerInMins FROM maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfigSheet
)
