-- FAIL if any lap lacks a matching driver in the same session
select
  l.session_key, l.driver_number, l.lap_number
from {{ ref('int_laps_enriched') }} l
left join {{ ref('stg_drivers') }} d
  on d.session_key = l.session_key
 and d.driver_number = l.driver_number
where d.driver_number is null
