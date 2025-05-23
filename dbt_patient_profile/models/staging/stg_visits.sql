select
    mrn,
    csn as pat_enc_csn_id,
    fin_class_name,
    appt_group,
    appt_status,
    appt_date,
    age,
    department_name,
    dept_specialty_name,
    center_name,
    visit_prov_id,
    visit_provider_name,
    visit_provider_type,
    row_number() over(partition by mrn order by appt_date desc) as rn
from {{ source('rpt', 'appointment') }}
where appt_status = 'Completed'
  and appt_date >= dateadd(month, datediff(month, 0, getdate()) - 36, 0)
