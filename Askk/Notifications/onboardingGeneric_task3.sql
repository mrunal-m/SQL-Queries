--task 3
TRUNCATE TABLE `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplateFinal` ;

INSERT INTO `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplateFinal` 
SELECT * FROM `maximal-furnace-783.askk_analytics.crmOnboardingGenericWithTemplate`
WHERE hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP() AT TIME ZONE "Asia/Kolkata")


