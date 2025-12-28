-- FAIL if pit duration is null, <= 0, or absurdly high (>120s)
select
  p.session_key, p.driver_number, p.lap_number, p.pit_duration_s
from {{ ref('stg_pit') }} p
where p.pit_duration_s is null
   or p.pit_duration_s <= 0
   or p.pit_duration_s > 120
