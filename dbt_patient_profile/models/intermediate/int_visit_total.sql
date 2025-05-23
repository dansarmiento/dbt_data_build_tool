with visits as (
    select * from {{ ref('stg_visits') }}
),
visit_scheduled as (
    select * from {{ ref('stg_visit_scheduled') }}
)
select
    visits.mrn,
    count(pat_enc_csn_id) as total_visits,
    case
        when count(case when appt_group = 'New' then 1 end) > 0
         and count(case when appt_group = 'Follow-Up' then 1 end) = 0
        then 1 else 0 end as new_patient,
    case
        when count(case when visits.appt_date >= dateadd(month, -12, getdate()) then 1 end) = 0
         and not exists (select 1 from visit_scheduled vs where vs.mrn = visits.mrn)
        then 'TRUE' else 'FALSE' end as "12_month_churn",
    case
        when count(case when visits.appt_date >= dateadd(month, -24, getdate()) then 1 end) = 0
         and not exists (select 1 from visit_scheduled vs where vs.mrn = visits.mrn)
        then 'TRUE' else 'FALSE' end as "24_month_churn"
from visits
group by visits.mrn
