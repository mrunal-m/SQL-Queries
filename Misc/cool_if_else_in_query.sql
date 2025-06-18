

IF (Select count(*) from `sc-bigquery-product-analyst.data_extraction.temp_vibely_icon_daily_pickup_rate`) > 0 THEN 
create table if not exists `sc-bigquery-product-analyst.data_extraction.vibely_icon_daily_pickup_rate` as 
(Select * from `sc-bigquery-product-analyst.data_extraction.temp_vibely_icon_daily_pickup_rate`);
delete from `sc-bigquery-product-analyst.data_extraction.vibely_icon_daily_pickup_rate`
where dt between start_date() and end_date();
Insert `sc-bigquery-product-analyst.data_extraction.vibely_icon_daily_pickup_rate`
Select * from `sc-bigquery-product-analyst.data_extraction.temp_vibely_icon_daily_pickup_rate`;
END IF;



