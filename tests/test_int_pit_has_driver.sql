-- FAIL if any pit event lacks a matching driver in the same session
select
  p.session_key, p.driver_number, p.lap_number, p.pit_time_utc
from {{ ref('int_pit_enriched') }} p
left join {{ ref('stg_drivers') }} d
  on d.session_key = p.session_key
 and d.driver_number = p.driver_number
where d.driver_number is null
