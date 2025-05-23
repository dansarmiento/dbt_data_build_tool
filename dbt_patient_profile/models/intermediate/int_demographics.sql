with visits as (select * from {{ ref('stg_visits') }})
select 
    v.mrn,
    v.fin_class_name,
    pt.sex_name as gender,
    pl.pt_language
from visits v
left join {{ source('src', 'patient') }} pt on v.mrn = pt.mrn
left join {{ source('src', 'patient_language') }} pl on v.mrn = pl.pat_mrn_id
where v.rn = 1
