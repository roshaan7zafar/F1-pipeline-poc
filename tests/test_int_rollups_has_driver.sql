-- FAIL if any driver-session rollup lacks a matching driver record
select
  r.session_key, r.driver_number
from {{ ref('int_driver_session_rollups') }} r
left join {{ ref('stg_drivers') }} d
  on d.session_key = r.session_key
 and d.driver_number = r.driver_number
where d.driver_number is null
