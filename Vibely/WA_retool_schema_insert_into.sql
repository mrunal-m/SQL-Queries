-- WARNING!!! date should be in timestamp() type but without UTC part
CREATE OR REPLACE TABLE `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_feed_down_20Apr`
  LIKE `maximal-furnace-783.vibely_analytics.crmDataWithTemplate`;

INSERT INTO `maximal-furnace-783.vibely_analytics.crmVibelyAdhocRetoolWA_feed_down_20Apr`

SELECT current_timestamp() AS date, 'adhoc' Cohort,  'English' language, '' userId, '9657295754' phoneNo, 'name' userName ,
'vibely_techissue_users_20apr25' template, -- Joseph provides
concat('{"header" : ["','https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/2945ed1e_1730205268033_sc.png','"]}')   as templateVariables,
'https://cdn-sc-g.sharechat.com/33d5318_1c8/tools/2945ed1e_1730205268033_sc.png' cdnUrl
--cdn is provided by Design
