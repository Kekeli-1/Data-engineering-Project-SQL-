select * from data_eng
alter table data_eng 
rename to raw_marketing_campaigns;
select * from raw_marketing_campaigns;
select count(*) from raw_marketing_campaigns;
select * from raw_marketing_campaigns;
select 
select sum(case when Clicks is null then 1 else 0 end) as null_clicks, sum(case when Impressions is null then 1 else 0 end) as null_impressions, sum(case when Date is null then 1 else 0 end) as null_date
from raw_marketing_campaigns;
select sum(case when Clicks < 1 then 1 else 0 end) as negative_clicks,sum(case when Clicks > Impressions then 1 else 0 end) as impossible_rows, sum(case when Date > CURRENT_DATE() then 1 else 0 end)
as future_dates from raw_marketing_campaigns;
create table stg_marketing_campaign as 
select `ï»¿Campaign_ID` , lower(trim(Campaign_Type)) as campaign_type, lower(trim(Target_Audience)) as target_audience, lower(trim(Company)) as company, lower(trim(Channel_Used)) as channel_used,
lower(trim(Location)) as location, lower(trim(Language)) as language, lower(trim(Customer_Segment)) as customer_segment, coalesce(Duration,0) as duration, coalesce(Conversion_Rate,0) as conversion_rate,
coalesce(Acquisition_Cost, 0) as aquisition_cost, coalesce(ROI,0) as ROI, coalesce(Clicks, 0) as clicks, coalesce(Impressions,0) as impressions, coalesce(Engagement_Score,0) as engagement_score,
str_to_date(Date, '%m/%d/%Y)') as campaign_date from (select * from raw_marketing_campaigns) t;
select * from stg_marketing_campaign; 
 
create table dim_campaign as select row_number() over (order by `ï»¿Campaign_ID`) as campaign_key, `ï»¿Campaign_ID`,campaign_type,target_audience,company, channel_used ,customer_segment from
(select distinct `ï»¿Campaign_ID`,campaign_type,target_audience,company,channel_used,customer_segment from stg_marketing_campaign
where `ï»¿Campaign_ID`is not null) t;
select * from dim_campaign;

create table dim_campaignss as select row_number() over (order by `ï»¿Campaign_ID`) as campaign_key, `ï»¿Campaign_ID`,campaign_type,target_audience,company, channel_used ,customer_segment,campaign_date from
(select distinct `ï»¿Campaign_ID`,campaign_type,target_audience,company,channel_used,customer_segment, campaign_date from stg_marketing_campaign
where `ï»¿Campaign_ID`is not null) t;

select * from dim_campaignss;
select `ï»¿Campaign_ID`, count(*) from dim_campaign 
group by `ï»¿Campaign_ID`
having count(*) > 1;
select count(*) as rows, count(distinct `ï»¿Campaign_ID`) as unique_keys from dim_campaign;

create table fact_campaign_performance as (select coalesce(dc.campaign_key,-1) as campaign_key, smc.campaign_date, smc.clicks, smc.impressions,smc.conversion_rate,smc.ROI,smc.aquisition_cost,smc.engagement_score, 
current_timestamp as created_at from stg_marketing_campaign smc
left join dim_campaign dc on smc.`ï»¿Campaign_ID` = dc.`ï»¿Campaign_ID`)
select * from fact_campaign_performance;
select campaign_key from fact_campaign_performance
where campaign_key < 0;

create table dim_date as select distinct campaign_date as date_key, extract(year from campaign_date) as year, extract(month from campaign_date) as month, extract(quarter from 
campaign_date),
current_timestamp as created_at from stg_marketing_campaign
where campaign_date is not null;
select * from dim_date;

drop table if exists fact_campaign_performance 

create table fact_campaign_performance as (select coalesce(dc.campaign_key, -1) as campaign_key,dd.date_key,coalesce(smc.conversion_rate,0) as conversion_rate,coalesce(smc.aquisition_cost,0) as acquisition_rate,
coalesce(smc.ROI,0) as ROI,coalesce(smc.clicks,0) as clicks,coalesce(smc.impressions,0) as impressions,coalesce(smc.engagement_score,0) as engagement_score, 
current_timestamp as created_at from stg_marketing_campaign smc 
left join dim_campaignss dc on smc.`ï»¿Campaign_ID` = dc.`ï»¿Campaign_ID`
left join dim_date dd on smc.campaign_date = dd.date_key) ;


select * from fact_campaign_performance;
select count(*) from stg_marketing_campaign 
select count(*) from fact_campaign_performance 

create index indx_fact_campaign_key on fact_campaign_performance(campaign_key);
create index indx_fact_date_key on fact_campaign_performance(date_key);
create index indx_dim_`ï»¿Campaign_ID`on dim_campaignss(`ï»¿Campaign_ID`)

create table fact_marketing_performances (select coalesce(dc.campaign_key, -1) as campaign_key,dd.date_key,coalesce(smc.conversion_rate,0) as conversion_rate,coalesce(smc.aquisition_cost,0) as acquisition_rate,
coalesce(smc.ROI,0) as ROI,coalesce(smc.clicks,0) as clicks,coalesce(smc.impressions,0) as impressions,coalesce(smc.engagement_score,0) as engagement_score, 
current_timestamp as created_at from stg_marketing_campaign smc 
left join dim_campaignss dc on smc.`ï»¿Campaign_ID` = dc.`ï»¿Campaign_ID`
left join dim_date dd on smc.campaign_date = dd.date_key
where smc.campaign_date > (select max(date_key) from fact_marketing_performance));

select * from dim_campaignss 
select * from fact_marketing_performances
select * from dim_date 

select `ï»¿Campaign_ID`, count(*) from dim_campaignss 
group by `ï»¿Campaign_ID`
having count(*) > 1;

select *  from fact_marketing_performances 
where campaign_key is null or date_key is null;

create table mart_campaign_kpiss as (select dc.campaign_type, dc.channel_used, dd.year,dd.month, sum(fcp.clicks) as total_clicks, sum(fcp.impressions) as impressions,
sum(fcp.acquisition_rate) as total_acquisition_cost, sum(fcp.ROI) as total_ROI, round(100 * (sum(fcp.clicks)/ sum(fcp.impressions)),2) as ctr
from fact_campaign_performance fcp 
join dim_campaignss dc on fcp.campaign_key = dc.campaign_key
join dim_date dd on fcp.date_key = dd.date_key
group by dc.campaign_type, dc.channel_used,dd.year,dd.month);

select * from mart_campaign_kpiss;
select channel_used, sum(total_clicks) as total_clicks, sum(impressions) as impressions from mart_campaign_kpiss 
group by channel_used
order by total_clicks desc;
