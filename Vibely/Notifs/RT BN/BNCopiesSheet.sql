-- sheet https://cdn-sc-g.sharechat.com/33d5318_1c8/1606b910_1771512238598_sc.png
TRUNCATE TABLE maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig;
INSERT INTO maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfig
(
SELECT *, '' fsIconImage, 0	timerInMins FROM maximal-furnace-783.vibely_analytics.crmBehaviouralNotificationCohortConfigSheet
)
