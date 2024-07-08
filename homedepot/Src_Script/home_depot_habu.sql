
-- Find line items that is associated to a random Home Depot campaign
SELECT 
StartDateTime,
EndDateTime,
id, 
name, 
status
  FROM `dl-datalake-bronze-prd.gam_dts_files_bronze.MatchTableLineItem_6881` 
  where lower(name) like '%home%depot%' and status = 'COMPLETED'
  and StartDateTime >= '2024-05-03' and EndDateTime < '2024-05-14'
  limit 1000;

--extract only the line item IDs
  SELECT 
distinct id
  FROM `dl-datalake-bronze-prd.gam_dts_files_bronze.MatchTableLineItem_6881` 
  where lower(name) like '%home%depot%' and status = 'COMPLETED'
  and StartDateTime >= '2024-05-03' and EndDateTime < '2024-05-14'
  limit 1000;


--extract impressions from pixel logs

create or replace table `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires` as 
select timestamp_utc,
ip,
device_id,
regexp_extract(url, r'line_item_id=([\d.]+)') line_item_id,
regexp_extract(url, r'advertiser_id=([\d.]+)') advertiser_id,
regexp_extract(url, r'creative_id=([\d.]+)') creative_id
 FROM `dl-datalake-bronze-prd.madhive_bronze.madhive_pixel_fires` where upper(beacon_name) like '%JIFFY%'
 and timestamp_utc > '2024-05-02' and beacon_name = 'Jiffy_Pixel'
 and cast(regexp_extract(url, r'line_item_id=([\d.]+)') as int) in (
  SELECT 
distinct id
  FROM `dl-datalake-bronze-prd.gam_dts_files_bronze.MatchTableLineItem_6881` 
  where lower(name) like '%home%depot%' and status = 'COMPLETED'
  and StartDateTime >= '2024-05-03' and EndDateTime < '2024-05-14');


--generate impression id
  create or replace table `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_unique` as 
select GENERATE_UUID() as impression_Id, * from `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires`;


--Add HEM
create or replace table `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_HEM` as 
select a.*, b.identifier as HEM from `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_unique` a 
left outer join 
(select distinct ip_address, identifier
from
dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest
where id_subtype = 'HEM') b
on a.ip = b.ip_address ;

--add HH ID

create or replace table `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_final` as 
select 
impression_id,
DATETIME(timestamp_utc,  "America/New_York")exposure_dtm_est,
 SHA256(ip) as hashed_ip,
 device_id, 
 line_item_id,
 advertiser_id,
 creative_id,
 HEM,
sha256(b.univision_hhld_id) as UHID from `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_HEM` a 
left outer join 
(select distinct ip_address, univision_hhld_id
from
dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest
) b
on a.ip = b.ip_address ;

select * from `dl-advertising-sales.scratch_kent.homedepotbrand_campaign_fires_final` where uhid is not null limit 1000;
