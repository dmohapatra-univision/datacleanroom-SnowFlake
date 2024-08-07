/****************************************************************FINAL OUTPUT.  *******************************************************/
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
  $$
CREATE or replace transient TABLE app_internal_data.hut
  AS 
select 
distinct 
date(to_char(BROADCAST_DS_6_AM_NY),  'YYYYMMDD')as event_date,
a.va_household_id,
b.extern_tuhhid,
WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT
 from SHARED_SCHEMA.V_VIEWERSHIP_WEEKLY_WT_PANEL_9_9 a,
 TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VA_TU_CROSSWALK b where
 a.va_household_id = b.va_household_id --replace with cleanroom table
and BROADCAST_DS_6_AM_NY >= {{ broadcast_start_ds_ny | sqlsafe }} 
and BROADCAST_DS_6_AM_NY < {{ broadcast_end_ds_ny | sqlsafe }}
and SESSION_DURATION_SECONDS >= 60;
$$);
-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
$$
CREATE or replace transient TABLE app_internal_data.tu_reach
  AS
select 
distinct 
date(to_char(BROADCAST_DS_6_AM_NY),  'YYYYMMDD') as event_date,
NORMALIZED_NETWORK,
a.va_household_id,
b.extern_tuhhid,
WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT
 from SHARED_SCHEMA.V_VIEWERSHIP_WEEKLY_WT_PANEL_9_9 a,
 TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VA_TU_CROSSWALK b where
 a.va_household_id = b.va_household_id 
{% if  network_list %}
AND normalized_network in ( {{  network_list[0]  }}  {% for network in network_list[1:] %} ,  {{ network  }}  {% endfor %})
{% endif %}
and BROADCAST_DS_6_AM_NY >= {{ broadcast_start_ds_ny | sqlsafe }} 
and BROADCAST_DS_6_AM_NY < {{ broadcast_end_ds_ny | sqlsafe }}
and SESSION_DURATION_SECONDS >= 60;
$$);
-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
$$
CREATE or replace transient TABLE app_internal_data.vix_overlap
 AS
select 
distinct
date(to_char(BROADCAST_DS_6_AM_NY),  'YYYYMMDD') as event_date,
a.va_household_id ,
b.extern_tuhhid,
WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT
 from SHARED_SCHEMA.V_VIEWERSHIP_WEEKLY_WT_PANEL_9_9 a, 
TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA b 
  where 
a.va_household_id =b.va_household_id
{% if  network_list %}
AND normalized_network in ( {{  network_list[0]  }}  {% for network in network_list[1:] %} ,  {{ network  }}  {% endfor %})
{% endif %}
and BROADCAST_DS_6_AM_NY >= {{ broadcast_start_ds_ny | sqlsafe }} 
and BROADCAST_DS_6_AM_NY < {{ broadcast_end_ds_ny | sqlsafe }}
and SESSION_DURATION_SECONDS >= 10;
$$);

-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
$$
with linear_only as (
  with intab as (
select extern_tuhhid
from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.hut
where event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
group by 1 having count(distinct event_date)>=datediff(day,date('{{ broadcast_start_ds_ny | sqlsafe }}'),date('{{ broadcast_end_ds_ny | sqlsafe }}'))*0.25),
intab_view as (
  select a.extern_tuhhid,  
  avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)avg_weekly_weight
  from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.tu_reach a,
  intab b
  where a.extern_tuhhid = b.extern_tuhhid
  and a.extern_tuhhid not in (select distinct extern_tuhhid from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap)
  and event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
  {% if  network_list %}
  AND normalized_network in ( {{  network_list[0]  }}  {% for network in network_list[1:] %} ,  {{ network  }}  {% endfor %})
  {% endif %} 
  group by 1
)
select 'Linear Only' as HH_Type ,count(extern_tuhhid), sum(avg_weekly_weight)HH_Count from intab_view
),
overlap_agg as (
  select extern_tuhhid, avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)as avg_weekly_weight from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap
    where event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}') group by 1
),
overlap as(
  select a.HH_Type, a.panel_model_vix, b.match_vix, b.total_vix,
  (a.panel_model_vix/match_vix)*total_vix as Overlap_HH from
  (select 'overlap' as HH_Type ,sum(avg_weekly_weight)as panel_model_vix from overlap_agg
 group by 1)a,
  (select 'overlap' as HH_Type, count(distinct extern_tuhhid) as match_vix, count(distinct ip_address) as total_vix from TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA 
  where event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}') group by 1)b 
  where a.HH_Type = b.HH_Type
),
digital_only as (
select 'Digital Only' as HH_Type, (total_vix-overlap_HH) as HH_Count from overlap
)
select HH_type, HH_Count from linear_only
union all
select HH_type, Overlap_HH from overlap
union all
select HH_type, HH_Count from digital_only;
$$);

-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
$$
select event_date,count(distinct va_household_id) as va_hhid_cnt,
count(distinct extern_tuhhid) as tuhhid_cnt from app_internal_data.hut
group by 1;
$$);

-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES
values ('{{ template_name | sqlsafe }}',
select event_date,count(distinct va_household_id) as va_hhid_cnt,
count(distinct extern_tuhhid) as tuhhid_cnt from app_internal_data.tu_reach group by 1;
$$);

-----------------------------------------------------------------------------------------------------------------------------
insert into TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.SHARED.PROPOSED_TEMPLATES

select event_date,count(distinct va_household_id) as va_hhid_cnt,
count(distinct extern_tuhhid) as tuhhid_cnt from app_internal_data.vix_overlap group by 1;

-----------------------------------------------------------------------------------------------------------------------------

with intab as (
select extern_tuhhid
from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.hut
where event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
group by 1 having count(distinct event_date)>=datediff(day,date('{{ broadcast_start_ds_ny | sqlsafe }}'),date('{{ broadcast_end_ds_ny | sqlsafe }}'))*0.25),
intab_view as (
  select a.extern_tuhhid,  
  avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)avg_weekly_weight
  from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.tu_reach a,
  intab b
  where a.extern_tuhhid = b.extern_tuhhid
  and a.extern_tuhhid not in (select distinct extern_tuhhid from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap)
  and event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
  {% if  network_list %}
  AND normalized_network in ( {{  network_list[0]  }}  {% for network in network_list[1:] %} ,  {{ network  }}  {% endfor %})
  {% endif %} 
  group by 1
)
select 'Linear Only' as HH_Type ,count(extern_tuhhid) as tuhhid_Count, sum(avg_weekly_weight) as HH_Count from intab_view;


-----------------------------------------------------------------------------------------------------------------------------

overlap_agg as (
  select extern_tuhhid, avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)as avg_weekly_weight from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap
    where event_date between date({{ broadcast_start_ds_ny | sqlsafe }}) and date({{ broadcast_end_ds_ny | sqlsafe }}) group by 1
)
select a.HH_Type, a.panel_model_vix, b.match_vix, b.total_vix,
(a.panel_model_vix/match_vix)*total_vix as Overlap_HH from
(select 'overlap' as HH_Type ,sum(avg_weekly_weight)as panel_model_vix from overlap_agg
group by 1)a,
(select 'overlap' as HH_Type, count(distinct extern_tuhhid) as match_vix, count(distinct ip_address) as total_vix from TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA 
where event_date between date({{ broadcast_start_ds_ny | sqlsafe }}) and date({{ broadcast_end_ds_ny | sqlsafe }}) group by 1)b 
where a.HH_Type = b.HH_Type;


----------------------------------------------------------------------------------------------------------------------------


select count(distinct extern_tuhhid) from (select extern_tuhhid
from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.hut
where event_date between date({{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
group by 1 having count(distinct event_date)>=datediff(day,date('{{ broadcast_start_ds_ny | sqlsafe }}'),date('{{ broadcast_end_ds_ny | sqlsafe }}'))*0.25)


----------------------------------------------------------------------------------------------------------------------------


CREATE or replace transient TABLE app_internal_data.va_intab_household
  AS
select distinct extern_tuhhid, va_household_id
from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.hut
where event_date between date('{{ broadcast_start_ds_ny | sqlsafe }}') and date('{{ broadcast_end_ds_ny | sqlsafe }}')
group by 1,2 having count(distinct event_date)>=datediff(day,date('{{ broadcast_start_ds_ny | sqlsafe }}'),date('{{ broadcast_end_ds_ny | sqlsafe }}'))*0.25
    

----------------------------------------------------------------------------------------------------------------------------

CREATE or replace transient TABLE app_internal_data.va_intab_linear_hh_only
  AS
select a.extern_tuhhid,  
  avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)avg_weekly_weight
  from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.tu_reach a,
  VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.va_intab_household b
  where a.extern_tuhhid = b.extern_tuhhid
  and a.extern_tuhhid not in (select distinct extern_tuhhid from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap)
 group by 1;


 ----------------------------------------------------------------------------------------------------------------------------

 with overlap_agg as (
  select extern_tuhhid, avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)as avg_weekly_weight from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap
  group by 1
)
select a.HH_Type, a.panel_model_vix, b.match_vix, b.total_vix,
  (a.panel_model_vix/match_vix)*total_vix as Overlap_HH from
  (select 'overlap' as HH_Type ,sum(avg_weekly_weight)as panel_model_vix from overlap_agg
 group by 1)a,
  (select 'overlap' as HH_Type, count(distinct extern_tuhhid) as match_vix, count(distinct ip_address) as total_vix from TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA 
  group by 1)b 
  where a.HH_Type = b.HH_Type;


  ----------------------------------------------------------------------------------------------------------------------------


with overlap_agg as (
  select extern_tuhhid, avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT) as avg_weekly_weight 
  from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap
  group by 1
),
overlap as (
  select a.HH_Type, a.panel_model_vix, b.match_vix, b.total_vix,
  (a.panel_model_vix/b.match_vix)*b.total_vix as Overlap_HH 
  from
    (select 'overlap' as HH_Type, sum(avg_weekly_weight) as panel_model_vix 
     from overlap_agg
     group by 1) a,
    (select 'overlap' as HH_Type, count(distinct extern_tuhhid) as match_vix, count(distinct ip_address) as total_vix 
     from TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA 
     group by 1) b 
  where a.HH_Type = b.HH_Type
)
select 'Digital Only' as HH_Type, (total_vix - Overlap_HH) as HH_Count 
from overlap   
union all
select HH_Type, Overlap_HH 
from overlap;  
  

  ----------------------------------------------------------------------------------------------------------------------------


with overlap_agg as (
  select extern_tuhhid, avg(WEEKLY_WEIGHTS_HOUSEHOLD_WEIGHT)as avg_weekly_weight from VIDEOAMP_TO_TELEVISAUNIVISION_DAAS_DCR.APP_INTERNAL_DATA.vix_overlap
  group by 1
),
overlap as (
  select a.HH_Type, a.panel_model_vix, b.match_vix, b.total_vix,
  (a.panel_model_vix/match_vix)*total_vix as Overlap_HH from
  (select 'overlap' as HH_Type ,sum(avg_weekly_weight)as panel_model_vix from overlap_agg
 group by 1)a,
  (select 'overlap' as HH_Type, count(distinct extern_tuhhid) as match_vix, count(distinct ip_address) as total_vix from TELEVISAUNIVISION_TO_VIDEOAMP_DAAS_DCR.MYDATA.VIX360_IP_TUHHID2_VA 
  group by 1)b 
  where a.HH_Type = b.HH_Type)

  select 'Digital Only' as HH_Type, (total_vix-overlap_HH) as HH_Count from overlap   
  union all
  select HH_type, Overlap_HH from overlap
  
  