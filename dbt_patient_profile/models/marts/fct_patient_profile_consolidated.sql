select
    t.mrn,
    p.pat_id,
    demo.age_bucket,
    demo.age_bucket_order,
    t.total_visits,
    d.min_appt_date
from {{ ref('int_visit_total') }} t
left join {{ source('cube', 'dim_patient') }} p on t.mrn = p.patient_mrn
left join {{ ref('int_dept_counts') }} d on d.mrn = t.mrn
left join {{ source('src', 'population_zipcode_irs') }} z on p.patient_zip_code = z.zip
left join {{ ref('int_demographics') }} demo on demo.mrn = t.mrn
left join {{ ref('stg_visit_scheduled') }} vs on vs.mrn = t.mrn and vs.rn = 1
