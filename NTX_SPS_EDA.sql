-- NTX_SPS_EDA_v2.sql regenerated on 2025-09-17
-- (Snowflake-safe casting; SPS-aligned core build + EDA basics)

set LOOKBACK_YEARS = 6;

create or replace temporary table NTMx_DAY_SUMMARY as
with med as (
  select ma.EncounterKey,
         cast(ma.AdministrationInstant as date) as cal_day,
         lower(coalesce(md.Route,'')) as route,
         lower(md.Name) as med_name
  from MedicationAdministrationFact ma
  join MedicationDim md on ma.MedicationKey = md.MedicationKey
  where ma.AdministrationInstant >= dateadd(year, -$LOOKBACK_YEARS, current_date)
)
select
  EncounterKey,
  cal_day,
  count(distinct case
    when (
      med_name ilike '%gentamicin%' or med_name ilike '%tobramycin%' or med_name ilike '%amikacin%' or
      med_name ilike '%vancomycin%' or med_name ilike '%acyclovir%'  or med_name ilike '%amphotericin%' or
      med_name ilike '%ibuprofen%'  or med_name ilike '%ketorolac%'
    ) and route not in ('ophthalmic','otic','topical','inhaled','intravesicular','intrathecal')
    then med_name end) as ntmx_count,
  boolor_agg( med_name ilike '%vancomycin%' and route in ('iv','intravenous') ) as vanc_iv_day,
  boolor_agg( (med_name ilike '%gentamicin%' or med_name ilike '%tobramycin%' or med_name ilike '%amikacin%')
              and route in ('iv','intravenous') ) as ag_iv_day
from med
group by EncounterKey, cal_day;

create or replace temporary table NTMx_DAY_SUMMARY3 as
select
  *,
  (ntmx_count >= 3)
  or (sum(iff(vanc_iv_day,1,0)) over (partition by EncounterKey order by cal_day rows 2 preceding) = 3)
  or (sum(iff(ag_iv_day,1,0))   over (partition by EncounterKey order by cal_day rows 2 preceding) = 3)
  as meets_exposure_day
from NTMx_DAY_SUMMARY;

create or replace temporary table NTMx_EXPOSURE_EVENTS as
with runs as (
  select *,
         case when coalesce(meets_exposure_day,false) then 0 else 1 end as gap_flag,
         sum(case when coalesce(meets_exposure_day,false) then 0 else 1 end)
           over (partition by EncounterKey order by cal_day) as grp
  from NTMx_DAY_SUMMARY3
)
select EncounterKey,
       min(cal_day) as exposure_start_day,
       max(cal_day) as exposure_end_day,
       dateadd(day,2,max(cal_day)) as risk_end_day
from runs
where meets_exposure_day
group by EncounterKey, grp;

create or replace temporary table BASELINE_SCR_6MO as
with scr as (
  select r.EncounterKey,
         r.ResultInstant::date as dte,
         cast(
           case when lower(coalesce(r.Unit,'')) in ('µmol/l','umol/l','μmol/l')
                then try_to_decimal(r.NumericValue,38,10)/88.4
                else try_to_decimal(r.NumericValue,38,10) end
           as double
         ) as scr_mgdl
  from LabComponentResultFact r
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
)
select e.EncounterKey,
       min(scr.scr_mgdl) as baseline_scr_mgdl,
       'lowest_6mo' as baseline_method
from NTMx_EXPOSURE_EVENTS e
join scr on scr.EncounterKey=e.EncounterKey
where scr.dte between dateadd(month,-6,e.exposure_start_day) and e.exposure_start_day
group by e.EncounterKey;

create or replace temporary table NAKI_EVENTS as
with series as (
  select e.EncounterKey,
         e.exposure_start_day, e.exposure_end_day, e.risk_end_day,
         r.ResultInstant::date as scr_day,
         cast(
           case when lower(coalesce(r.Unit,'')) in ('µmol/l','umol/l','μmol/l')
                then try_to_decimal(r.NumericValue,38,10)/88.4
                else try_to_decimal(r.NumericValue,38,10) end
           as double
         ) as scr_mgdl
  from NTMx_EXPOSURE_EVENTS e
  join LabComponentResultFact r on r.EncounterKey=e.EncounterKey
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
    and r.ResultInstant::date between e.exposure_start_day and e.risk_end_day
), joined as (
  select s.*, b.baseline_scr_mgdl
  from series s left join BASELINE_SCR_6MO b using (EncounterKey)
), kdigo as (
  select *,
         case
           when scr_mgdl >= 0.5 and scr_mgdl >= baseline_scr_mgdl * 1.5 then '50pct'
           when scr_mgdl >= 0.5 and scr_mgdl - baseline_scr_mgdl >= 0.3 then '0.3mgdl'
           else null end as kdigo_delta_type
  from joined
)
select EncounterKey,
       min(scr_day) as first_naki_day,
       max(scr_mgdl) as peak_scr_mgdl,
       any_value(kdigo_delta_type) as kdigo_delta_type
from kdigo
where kdigo_delta_type is not null
group by EncounterKey;

create or replace temporary table UNIT_ATTRIBUTION as
select n.EncounterKey, n.first_naki_day,
       any_value(pl.UnitName) as unit_at_aki
from NAKI_EVENTS n
left join PatientLocationEventFact pl
  on pl.EncounterKey=n.EncounterKey and pl.EventInstant::date = n.first_naki_day
group by n.EncounterKey, n.first_naki_day;

create or replace view SCR_MONITORING as
with expected as (
  select EncounterKey, day_in_window
  from (
    select e.EncounterKey,
           dateadd(day, seq4(), e.exposure_start_day) as day_in_window
    from NTMx_EXPOSURE_EVENTS e, table(generator(rowcount => 10000)) g
    where dateadd(day, seq4(), e.exposure_start_day) <= e.risk_end_day
  )
),
observed as (
  select distinct e.EncounterKey, r.ResultInstant::date as day_in_window
  from NTMx_EXPOSURE_EVENTS e
  join LabComponentResultFact r on r.EncounterKey=e.EncounterKey
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
    and r.ResultInstant::date between e.exposure_start_day and e.risk_end_day
)
select e.EncounterKey,
       count(*) as expected_days,
       count(o.day_in_window) as observed_days,
       round(100.0*count(o.day_in_window)/nullif(count(*),0),1) as pct_compliance
from expected e
left join observed o
  on e.EncounterKey=o.EncounterKey and e.day_in_window=o.day_in_window
group by e.EncounterKey;

create or replace view EDA_COHORT_BY_YEAR as
select year(exposure_start_day) as year,
       count(distinct EncounterKey) as exposed_encounters
from NTMx_EXPOSURE_EVENTS
group by year order by year;

create or replace view EDA_EXPOSURE_DURATION as
select EncounterKey,
       datediff('day', exposure_start_day, exposure_end_day)+1 as exposure_days
from NTMx_EXPOSURE_EVENTS;

create or replace view EDA_TIME_TO_NAKI as
select e.EncounterKey,
       datediff('day', e.exposure_start_day, n.first_naki_day) as days_to_aki
from NTMx_EXPOSURE_EVENTS e
join NAKI_EVENTS n using (EncounterKey);

create or replace view EDA_UNIT_NAKI as
select coalesce(u.unit_at_aki,'UNKNOWN') as unit_at_aki,
       count(*) as naki_count
from UNIT_ATTRIBUTION u
group by unit_at_aki
order by naki_count desc;

create or replace view EDA_NAKI_INCIDENCE as
select (select count(*) from NAKI_EVENTS) as naki_encounters,
       (select count(*) from NTMx_EXPOSURE_EVENTS) as exposure_events;
