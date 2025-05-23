select
    mrn,
    visit_prov_id,
    dept_specialty_name,
    count(pat_enc_csn_id) as visit_count,
    min(appt_date) as min_appt_date
from {{ ref('stg_visits') }}
where mrn in (select mrn from {{ ref('int_visit_total') }} where total_visits > 0)
group by mrn, visit_prov_id, dept_specialty_name
