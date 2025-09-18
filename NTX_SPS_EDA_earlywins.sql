-- =====================================================================
-- NTX_SPS_EDA_earlywins.sql
-- Early-win EDA views layered on the SPS-aligned build
-- Author: ChatGPT (GPT-5)
-- Date: 2025-09-18
-- =====================================================================

/* 1) Exposure burden: distribution of exposure days per encounter */
create or replace view EDA_EXPOSURE_DISTRIBUTION as
select
  datediff('day', exposure_start_day, exposure_end_day) + 1 as exposure_days,
  count(distinct EncounterKey) as n_encounters
from NTMx_EXPOSURE_EVENTS
group by exposure_days
order by exposure_days;

/* 2) Risky drug pairs within exposure window (vanc + pip/tazo or aminoglycoside) */
create or replace view EDA_RISKY_PAIRS as
select
  e.EncounterKey,
  boolor_agg(lower(md.Name) like '%vancomycin%') as vanc_flag,
  boolor_agg(lower(md.Name) like '%piperacillin%tazobactam%') as piptazo_flag,
  boolor_agg(lower(md.Name) like '%gentamicin%' or lower(md.Name) like '%tobramycin%' or lower(md.Name) like '%amikacin%') as ag_flag
from NTMx_EXPOSURE_EVENTS e
join MedicationAdministrationFact ma on e.EncounterKey=ma.EncounterKey
join MedicationDim md on ma.MedicationKey=md.MedicationKey
where ma.AdministrationInstant::date between e.exposure_start_day and e.exposure_end_day
group by e.EncounterKey;

/* 2b) Overlay AKI outcome on risky pairs */
create or replace view EDA_RISKY_PAIR_OUTCOMES as
select
  r.EncounterKey,
  r.vanc_flag,
  r.piptazo_flag,
  r.ag_flag,
  iff(n.EncounterKey is not null, 1, 0) as aki_flag
from EDA_RISKY_PAIRS r
left join NAKI_EVENTS n using (EncounterKey);

/* 3) Monitoring gaps: SCr daily compliance summary */
create or replace view EDA_SCR_COMPLIANCE as
select
  round(avg(pct_compliance),1) as avg_pct_compliance,
  sum(case when pct_compliance < 100 then 1 else 0 end) as encounters_with_gaps,
  count(*) as total_encounters
from SCR_MONITORING;

/* 4) Nares PCR negatives among vanc exposures (lab-only proxy) */
create or replace view EDA_NARES_NEGATIVE_VANC as
select
  count(*) as vanc_exposure_events,
  sum(case when nares_negative_flag=1 then 1 else 0 end) as nares_negative_events,
  sum(case when nares_negative_flag=1 and deescalate_by_nares=1 then 1 else 0 end) as deescalation_eligible_by_nares
from VANC_APPROPRIATENESS_LABONLY;

/* 5) 72h culture-negative among vanc exposures (lab-only proxy) */
create or replace view EDA_72H_CULTURE_NEG as
select
  count(*) as vanc_exposure_events,
  sum(case when any_culture_72h=1 then 1 else 0 end) as had_culture_72h,
  sum(case when culture_negative_72h=1 then 1 else 0 end) as culture_negative_72h_events,
  sum(case when culture_negative_72h=1 and deescalate_by_72h=1 then 1 else 0 end) as deescalation_eligible_by_72h
from VANC_APPROPRIATENESS_LABONLY;

/* 6) Outcome contrast: LOS by AKI vs non-AKI */
create or replace view EDA_LOS_BY_AKI as
select
  iff(n.EncounterKey is not null, 'AKI','No AKI') as aki_flag,
  avg(datediff('day', ef.AdmissionInstant, ef.DischargeInstant)) as avg_los_days
from EncounterFact ef
left join NAKI_EVENTS n using (EncounterKey)
group by aki_flag;

/* 7) Readmissions (30-day) by AKI vs non-AKI */
create or replace view EDA_READMISSION_BY_AKI as
with disch as (
  select ef.EncounterKey, ef.PatientKey, ef.DischargeInstant
  from EncounterFact ef
)
select
  iff(n.EncounterKey is not null, 'AKI','No AKI') as aki_flag,
  count(distinct d1.EncounterKey) as index_encounters,
  sum(case when exists (
    select 1 from disch d2
    where d2.PatientKey=d1.PatientKey
      and d2.DischargeInstant > d1.DischargeInstant
      and d2.DischargeInstant <= dateadd(day,30,d1.DischargeInstant)
  ) then 1 else 0 end) as readmissions_30d
from disch d1
left join NAKI_EVENTS n on n.EncounterKey=d1.EncounterKey
group by aki_flag;

/* 8) Unit hotspots for NAKI */
create or replace view EDA_UNIT_HOTSPOTS as
select
  coalesce(u.unit_at_aki,'UNKNOWN') as unit_at_aki,
  count(*) as naki_count
from UNIT_ATTRIBUTION u
group by unit_at_aki
order by naki_count desc;
