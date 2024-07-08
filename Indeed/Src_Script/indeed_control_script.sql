create or replace table `dl-advertising-sales.scratch_kent.TU_Spine_Demo` as
select
distinct
a.extern_tuhhid, 
ma1694 as individual_age,
ma1697 as household_income,
case when (audience_402264 = 1 or audience_402265 = 1 or audience_402266 = 1 or audience_402267 = 1 or audience_402268=1) then 1 else 0 end as presence_of_children,
 case when audience_403322 = 1 then 1 else 0 end  as Age_18_to_24,
 case when audience_403323 = 1 then 1 else 0 end as Age_25_to_34,
 case when audience_403324 = 1 then 1 else 0 end as Age_35_to_44,
 case when audience_403325 = 1 then 1 else 0 end as Age_45_to_54,
 case when audience_403326 = 1 then 1 else 0 end as Age_55_to_64,
 case when audience_403327 = 1 or audience_403328 = 1 then 1 else 0 end as Age_65_plus,
 case when audience_404026 = 1 then 1 else 0 end as Single,
 case when audience_404027 = 1 then 1 else 0 end as Married,
case when audience_402229 = 1 then 1 else 0 end as HHIncome_Than_35K,
case when audience_402230 = 1 then 1 else 0 end as HHIncome_35000_to_44999,
case when audience_402231 = 1 then 1 else 0 end as HHIncome_45000_to_54999,
case when audience_402232 = 1 then 1 else 0 end as HHIncome_55000_to_69999,
case when audience_402233 = 1 then 1 else 0 end as HHIncome_70000_to_84999,
case when audience_402234 = 1 then 1 else 0 end as HHIncome_85000_to_99999,
case when audience_402235 = 1 then 1 else 0 end as HHIncome_100000_to_124999,
case when audience_402236 = 1 then 1 else 0 end as HHIncome_125000_to_149999,
case when audience_402237 = 1 then 1 else 0 end as HHIncome_150000_to_199999,
case when audience_402238 = 1 then 1 else 0 end as HHIncome_200000_plus,
case when audience_403968 = 1 then 1 else 0 end as Male,
case when audience_402239 = 1 then 1 else 0 end as Female,
case when audience_404801 = 1 then 1 else 0 end as Some_College,
case when audience_404802 = 1 then 1 else 0 end as College_Grad,
case when audience_404800 = 1 then 1 else 0 end as High_School,
case when audience_404803 = 1 then 1 else 0 end as Graduate_School
from `dl-datalake-silver-prd.TU_spine_oneid_silver.tu_spine_adadvisor_append_oneid_silver` a,
`dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest` b 
where a.Extern_TUHHID = b.EXTERN_TUHHID;

create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49` as  
select distinct extern_tuhhid from `dl-advertising-sales.scratch_kent.TU_Spine_Demo`
where Age_18_to_24 = 1 or Age_25_to_34 = 1 or Age_35_to_44 = 1 or Age_45_to_54 = 1;

select count(1) from `dl-advertising-sales.scratch_kent.indeed_A18_49` limit 100; --18940167


create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49_control` as
select distinct extern_tuhhid from  `dl-advertising-sales.scratch_kent.indeed_A18_49`
limit 1894917;


create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49_test` as
select distinct extern_tuhhid from  `dl-advertising-sales.scratch_kent.indeed_A18_49`
where extern_tuhhid not in (select extern_tuhhid from `dl-advertising-sales.scratch_kent.indeed_A18_49_control`);

create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49_segment` as
select a.*, b.ip_address,id_subtype,b. identifier, 'control' as segment_type from `dl-advertising-sales.scratch_kent.indeed_A18_49_control` a
left outer join dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest b on a.extern_tuhhid = b.extern_tuhhid
union all
select a.*, b.ip_address,id_subtype,b. identifier, 'test' as segment_type from `dl-advertising-sales.scratch_kent.indeed_A18_49_test` a
left outer join dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest b on a.extern_tuhhid = b.extern_tuhhid;


select * from `dl-advertising-sales.scratch_kent.indeed_A18_49_segment` where extern_tuhhid = '1629ZBnTpsLWz0AZZsat6MmvHQ==' and id_subtype = 'HEM' limit 1000;

create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49_segment_final` as
select
sha256(extern_tuhhid) as uhid,
sha256(ip_address) as hashed_ip,
id_subtype,
identifier,
segment_type
from `dl-advertising-sales.scratch_kent.indeed_A18_49_segment`
where id_subtype = 'HEM';

select segment_type, count(distinct uhid) from `dl-advertising-sales.scratch_kent.indeed_A18_49_segment_final`
group by 1 limit 1000;


--Create suppression table with all devices

create or replace table `dl-advertising-sales.scratch_kent.indeed_A18_49_segment_suppress_Q3` as
select
sha256(extern_tuhhid) as uhid,
sha256(ip_address) as hashed_ip,
id_subtype,
identifier,
segment_type
from `dl-advertising-sales.scratch_kent.indeed_A18_49_segment`
where segment_type = 'control';


select * from `dl-advertising-sales.scratch_kent.indeed_A18_49_segment_suppress_Q3` limit 1000;