-- NTX_SPS_EDA_v3_nomicro.sql regenerated on 2025-09-17
-- MRSA / culture logic using only Lab tables

create or replace temporary view VANC_IV_STARTS as
with med as (
  select ma.EncounterKey,
         cast(ma.AdministrationInstant as date) as cal_day,
         lower(coalesce(md.Route,'')) as route,
         lower(md.Name) as med_name
  from MedicationAdministrationFact ma
  join MedicationDim md on ma.MedicationKey = md.MedicationKey
)
select EncounterKey, min(cal_day) as vanc_iv_first_day
from med
where med_name ilike '%vancomycin%' and route in ('iv','intravenous')
group by EncounterKey;

create or replace temporary view MRSA_NARES_AROUND_START as
select v.EncounterKey,
       r.ResultInstant::date as result_day,
       lower(coalesce(lcd.Name,'')) as lab_name,
       lower(coalesce(r.Value,'')) as result_text,
       iff( lab_name like '%nares%' and lab_name like '%mrsa%' and (lab_name like '%pcr%' or lab_name like '%screen%'), 1, 0) as is_mrsa_nares_test,
       iff( lower(coalesce(r.Value,'')) like '%negative%' or lower(coalesce(r.Value,'')) like '%not detected%', 1, 0) as is_negative
from VANC_IV_STARTS v
join LabComponentResultFact r on r.EncounterKey=v.EncounterKey
join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
where r.ResultInstant::date between dateadd(day,-7,v.vanc_iv_first_day) and dateadd(day,3,v.vanc_iv_first_day);

create or replace temporary view MRSA_NARES_DECISION as
select EncounterKey,
       max( case when is_mrsa_nares_test=1 then 1 else 0 end ) as any_nares_test,
       max( case when is_mrsa_nares_test=1 and is_negative=1 then 1 else 0 end ) as any_nares_negative
from MRSA_NARES_AROUND_START
group by EncounterKey;

create or replace temporary view CULTURE_72H_LAB as
with win as (
  select EncounterKey, vanc_iv_first_day,
         dateadd(hour,48, vanc_iv_first_day)::date as d48,
         dateadd(hour,72, vanc_iv_first_day)::date as d72
  from VANC_IV_STARTS
),
lab as (
  select w.EncounterKey,
         r.ResultInstant::date as result_day,
         lower(coalesce(lcd.Name,'')) as lab_name,
         lower(coalesce(r.Value,'')) as result_text
  from win w
  join LabComponentResultFact r on r.EncounterKey=w.EncounterKey
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where r.ResultInstant::date between w.d48 and w.d72
)
select EncounterKey,
       max( case when lab_name like '%culture%' and (result_text like '%staphylococcus aureus%' and (result_text like '%mrsa%' or result_text like '%methicillin-resistant%')) then 1 else 0 end ) as mrsa_positive_72h,
       max( case when lab_name like '%culture%' and (result_text like '%no growth%' or result_text like '%negative%' or result_text like '%normal flora%') then 1 else 0 end ) as culture_negative_72h,
       max( case when lab_name like '%culture%' then 1 else 0 end ) as any_culture_72h
from lab
group by EncounterKey;

create or replace view VANC_APPROPRIATENESS_LABONLY as
select e.EncounterKey,
       e.exposure_start_day,
       coalesce(n.any_nares_test,0) as any_nares_test,
       coalesce(n.any_nares_negative,0) as nares_negative_flag,
       coalesce(c.any_culture_72h,0) as any_culture_72h,
       coalesce(c.mrsa_positive_72h,0) as mrsa_positive_72h,
       coalesce(c.culture_negative_72h,0) as culture_negative_72h,
       iff(coalesce(n.any_nares_negative,0)=1, 1, 0) as deescalate_by_nares,
       iff(coalesce(c.any_culture_72h,0)=1 and coalesce(c.mrsa_positive_72h,0)=0 and coalesce(c.culture_negative_72h,0)=1, 1, 0) as deescalate_by_72h
from NTMx_EXPOSURE_EVENTS e
left join VANC_IV_STARTS v using (EncounterKey)
left join MRSA_NARES_DECISION n on n.EncounterKey=e.EncounterKey
left join CULTURE_72H_LAB c on c.EncounterKey=e.EncounterKey;

create or replace view EDA_VANC_DEESCALATION_ELIGIBILITY as
select count(*) as vanc_exposure_events,
       sum(iff(deescalate_by_nares=1,1,0)) as nares_neg_eligible,
       sum(iff(deescalate_by_72h=1,1,0)) as culture_72h_eligible,
       round(100.0 * sum(iff(deescalate_by_nares=1,1,0))/nullif(count(*),0),1) as pct_nares_neg_eligible,
       round(100.0 * sum(iff(deescalate_by_72h=1,1,0))/nullif(count(*),0),1) as pct_culture_72h_eligible
from VANC_APPROPRIATENESS_LABONLY;
