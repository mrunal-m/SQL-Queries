-- sheet https://docs.google.com/spreadsheets/d/1iG9G1NEu0kRspGcrL4uMwq3-W2b4entVueBtUn66ZNE/edit?gid=1226323505#gid=1226323505
TRUNCATE TABLE `maximal-furnace-783.askk_analytics.crmBehaviouralNotificationCohortConfig`;
INSERT INTO maximal-furnace-783.askk_analytics.crmBehaviouralNotificationCohortConfig
(
SELECT *, '' fsBGImage, '' fsIconImage, 0	timerInMins, '' templateVariables FROM maximal-furnace-783.askk_analytics.crmBehaviouralNotificationCohortConfigSheet
)
