select
    mrn,
    appt_date as next_scheduled_appt,
    row_number() over(partition by mrn order by appt_date asc) as rn
from {{ source('rpt', 'appointment') }}
where appt_status = 'Scheduled'
  and appt_date >= dateadd(month, datediff(month, 0, getdate()) - 36, 0)
