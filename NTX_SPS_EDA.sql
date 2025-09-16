-- =====================================================================
-- SPS-Aligned Nephrotoxin (NTMx) Exposure, NAKI Detection, and EDA Views
-- Author: ChatGPT (GPT-5 Thinking)
-- Date: 2025-09-16
--
-- Purpose:
--   End-to-end Snowflake SQL to:
--     1) Build SPS-compliant exposure cohorts and risk windows
--     2) Compute Baseline SCr (6-month lowest; unit-normalized)
--     3) Detect NAKI (KDIGO delta WITH 0.5 mg/dL peak floor) within risk window
--     4) Attribute NAKI to unit at draw time
--     5) Compute SCr monitoring compliance (expected vs observed days)
--     6) Provide EDA-ready views answering leadership & stewardship questions
--
-- Key References (from the uploaded PDF):
--   - SPS NAKI Operational Definition (exposure rules, risk window, AKI definition,
--     baseline SCr determination, location attribution, monitoring compliance)
--
-- Notes:
--   * Replace table/column names if your Caboodle layer differs.
--   * This script favors clarity and SPS traceability over hyper-optimization.
--   * If Schwartz fallback is needed, add height-based calc where noted.
-- =====================================================================

-- =============================
-- 0) Parameters
-- =============================
-- Lookback in years for exposure cohort (use 5–6 years for EDA)
set LOOKBACK_YEARS = 6;

-- =============================
-- 1) Daily exposure summary (calendar grain; SPS route exclusions)
--    Exposure day if:
--      - Distinct nephrotoxic meds (NTMx) on that calendar day >= 3 (any route except excluded routes)
--      - OR cumulative IV days of vancomycin in any 3 consecutive days
--      - OR cumulative IV days of aminoglycoside in any 3 consecutive days
--    Excluded routes (examples): topical, otic, ophthalmic, inhaled, intravesicular, intrathecal
-- =============================
create or replace temporary table NTMx_DAY_SUMMARY as
with med as (
  select
    ma.EncounterKey,
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
  /* distinct NTMx that day (exclude topical/otic/etc) */
  count(distinct case
    when med_name ilike any (
         '%gentamicin%','%tobramycin%','%amikacin%',
         '%vancomycin%','%acyclovir%','%amphotericin%',
         '%ibuprofen%','%ketorolac%')
     and route not in ('ophthalmic','otic','topical','inhaled','intravesicular','intrathecal')
    then med_name end) as ntmx_count,
  /* day-level flags by route */
  boolor_agg( med_name ilike '%vancomycin%' and route in ('iv','intravenous') ) as vanc_iv_day,
  boolor_agg( (med_name ilike '%gentamicin%' or med_name ilike '%tobramycin%' or med_name ilike '%amikacin%')
              and route in ('iv','intravenous') ) as ag_iv_day
from med
group by EncounterKey, cal_day;

create or replace temporary table NTMx_DAY_SUMMARY3 as
select
  *,
  /* SPS exposure on this day:
     - ≥3 NTMx today OR
     - 3-day rolling sum of vanc IV days reaches 3 OR
     - 3-day rolling sum of AG IV days reaches 3
  */
  (ntmx_count >= 3)
  or (sum(iff(vanc_iv_day,1,0)) over (partition by EncounterKey order by cal_day
      rows between 2 preceding and current row) = 3)
  or (sum(iff(ag_iv_day,1,0))   over (partition by EncounterKey order by cal_day
      rows between 2 preceding and current row) = 3)
  as meets_exposure_day
from NTMx_DAY_SUMMARY;

-- =============================
-- 2) Exposure events + risk window (+2 days post exposure)
-- =============================
create or replace temporary table NTMx_EXPOSURE_EVENTS as
with runs as (
  select
    *,
    case when coalesce(meets_exposure_day,false) then 0 else 1 end as gap_flag,
    sum( case when coalesce(meets_exposure_day,false) then 0 else 1 end )
      over (partition by EncounterKey order by cal_day) as grp
  from NTMx_DAY_SUMMARY3
)
select
  EncounterKey,
  min(cal_day) as exposure_start_day,
  max(cal_day) as exposure_end_day,
  dateadd(day,2,max(cal_day)) as risk_end_day
from runs
where meets_exposure_day
group by EncounterKey, grp;

-- =============================
-- 3) Baseline SCr (lowest in prior 6 months; unit-normalized to mg/dL)
--    Neonatal rule and Schwartz fallback can be appended where noted.
-- =============================
create or replace temporary table BASELINE_SCR_6MO as
with scr as (
  select
    r.EncounterKey,
    r.ResultInstant::date as dte,
    case when lower(coalesce(r.Unit,'')) in ('µmol/l','umol/l','μmol/l')
         then try_to_number(r.NumericValue)/88.4
         else try_to_number(r.NumericValue) end as scr_mgdl
  from LabComponentResultFact r
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
)
select
  e.EncounterKey,
  min(scr.scr_mgdl) as baseline_scr_mgdl,  -- lowest in window
  'lowest_6mo' as baseline_method
from NTMx_EXPOSURE_EVENTS e
join scr on scr.EncounterKey=e.EncounterKey
where scr.dte between dateadd(month,-6,e.exposure_start_day) and e.exposure_start_day
group by e.EncounterKey;

/* OPTIONAL (fallback): If baseline missing, compute Schwartz-based estimate:
   baseline_scr_mgdl = (0.413 * height_cm) / 120
   Neonates: use first SCr after day-of-life 3.
*/

-- =============================
-- 4) NAKI detection (within SPS risk window)
--    AKI if KDIGO delta met AND peak SCr >= 0.5 mg/dL
-- =============================
create or replace temporary table NAKI_EVENTS as
with series as (
  select
    e.EncounterKey,
    e.exposure_start_day, e.exposure_end_day, e.risk_end_day,
    r.ResultInstant::date as scr_day,
    case when lower(coalesce(r.Unit,'')) in ('µmol/l','umol/l','μmol/l')
         then try_to_number(r.NumericValue)/88.4
         else try_to_number(r.NumericValue) end as scr_mgdl
  from NTMx_EXPOSURE_EVENTS e
  join LabComponentResultFact r on r.EncounterKey=e.EncounterKey
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
    and r.ResultInstant::date between e.exposure_start_day and e.risk_end_day
), joined as (
  select s.*, b.baseline_scr_mgdl
  from series s
  left join BASELINE_SCR_6MO b using (EncounterKey)
), kdigo as (
  select
    *,
    /* Two KDIGO SCr criteria + 0.5 mg/dL peak floor */
    case
      when scr_mgdl >= 0.5 and scr_mgdl >= baseline_scr_mgdl * 1.5 then '50pct'
      when scr_mgdl >= 0.5 and scr_mgdl - baseline_scr_mgdl >= 0.3 then '0.3mgdl'
      else null
    end as kdigo_delta_type
  from joined
)
select
  EncounterKey,
  min(scr_day) as first_naki_day,
  max(scr_mgdl) as peak_scr_mgdl,
  any_value(kdigo_delta_type) as kdigo_delta_type
from kdigo
where kdigo_delta_type is not null
group by EncounterKey;

-- =============================
-- 5) Unit attribution (unit at time of first NAKI SCr draw)
-- =============================
create or replace temporary table UNIT_ATTRIBUTION as
select
  n.EncounterKey,
  n.first_naki_day,
  any_value(pl.UnitName) as unit_at_aki
from NAKI_EVENTS n
left join PatientLocationEventFact pl
  on pl.EncounterKey=n.EncounterKey
 and pl.EventInstant::date = n.first_naki_day
group by n.EncounterKey, n.first_naki_day;

-- =============================
-- 6) SCr monitoring compliance: expected vs observed draws
--    Expected: daily during exposure window + 2 days after
-- =============================
create or replace view SCR_MONITORING as
with expected as (
  select EncounterKey, day_in_window
  from (
    select
      e.EncounterKey,
      dateadd(day, seq4(), e.exposure_start_day) as day_in_window
    from NTMx_EXPOSURE_EVENTS e,
         table(generator(rowcount => 10000)) g
    where dateadd(day, seq4(), e.exposure_start_day) <= e.risk_end_day
  )
),
observed as (
  select distinct
    e.EncounterKey,
    r.ResultInstant::date as day_in_window
  from NTMx_EXPOSURE_EVENTS e
  join LabComponentResultFact r on r.EncounterKey=e.EncounterKey
  join LabComponentDim lcd on r.LabComponentKey=lcd.LabComponentKey
  where lcd.Name ilike '%creatinine%'
    and r.ResultInstant::date between e.exposure_start_day and e.risk_end_day
)
select
  e.EncounterKey,
  count(*) as expected_days,
  count(o.day_in_window) as observed_days,
  round(100.0*count(o.day_in_window)/nullif(count(*),0),1) as pct_compliance
from expected e
left join observed o
  on e.EncounterKey=o.EncounterKey and e.day_in_window=o.day_in_window
group by e.EncounterKey;

-- =============================
-- 7) EDA Views (quick answers to guiding questions)
--    NOTE: Add joins to PatientDim/EncounterFact/Microbiology/etc. in your environment.
-- =============================

-- 7.1 Cohort size by year
create or replace view EDA_COHORT_BY_YEAR as
select
  year(exposure_start_day) as year,
  count(distinct EncounterKey) as exposed_encounters
from NTMx_EXPOSURE_EVENTS
group by year
order by year;

-- 7.2 Exposure duration per event (with outliers)
create or replace view EDA_EXPOSURE_DURATION as
select
  EncounterKey,
  datediff('day', exposure_start_day, exposure_end_day) + 1 as exposure_days
from NTMx_EXPOSURE_EVENTS;

-- 7.3 Risky pairs rate per day (requires drug-day flags; sample placeholder columns)
-- You can build a per-day per-encounter drug-flag table and compute co-presence.
-- Example skeleton shown here:
create or replace view EDA_RISKY_PAIRS_DAILY as
select
  cal_day,
  count_if(true) as exposure_days_total,
  -- Replace the two lines below with real vanc/piptazo day flags from your med-day expansion
  sum(iff(false,1,0)) as vanc_ptz_days,
  sum(iff(false,1,0)) as vanc_aminoglycoside_days,
  1.0 * sum(iff(false,1,0)) / nullif(count(*),0) as vanc_ptz_rate,
  1.0 * sum(iff(false,1,0)) / nullif(count(*),0) as vanc_ag_rate
from NTMx_DAY_SUMMARY3
group by cal_day;

-- 7.4 Time to first NAKI (days)
create or replace view EDA_TIME_TO_NAKI as
select
  e.EncounterKey,
  datediff('day', e.exposure_start_day, n.first_naki_day) as days_to_aki
from NTMx_EXPOSURE_EVENTS e
join NAKI_EVENTS n using (EncounterKey);

-- 7.5 SCr monitoring compliance (already a view)
-- select * from SCR_MONITORING;

-- 7.6 Unit-level NAKI counts (join to patient-days for rates externally)
create or replace view EDA_UNIT_NAKI as
select
  coalesce(u.unit_at_aki, 'UNKNOWN') as unit_at_aki,
  count(*) as naki_count
from UNIT_ATTRIBUTION u
group by unit_at_aki
order by naki_count desc;

-- 7.7 Overall NAKI incidence (encounter level; one per event)
create or replace view EDA_NAKI_INCIDENCE as
select
  (select count(*) from NAKI_EVENTS) as naki_encounters,
  (select count(*) from NTMx_EXPOSURE_EVENTS) as exposure_events;

-- 7.8 Placeholders for microbiology appropriateness (requires your micro tables)
-- Example: Vanc without MRSA growth at 48–72h, antibiogram mismatch, etc.
-- Implement by joining NTMx_EXPOSURE_EVENTS to your MicrobiologyFact & Susceptibility tables.

-- =============================
-- 8) Quick sanity peeks
-- =============================
-- select * from EDA_COHORT_BY_YEAR limit 20;
-- select * from EDA_EXPOSURE_DURATION limit 20;
-- select * from EDA_TIME_TO_NAKI limit 20;
-- select * from EDA_UNIT_NAKI limit 20;
-- select * from SCR_MONITORING limit 20;

-- =====================================================================
-- End of script
-- =====================================================================
