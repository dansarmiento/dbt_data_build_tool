-- simplified example placeholder
select
    mrn,
    min(min_appt_date) as min_appt_date
from {{ ref('int_visit_agg') }}
group by mrn
