-- FAIL if clean laps have null or unrealistic lap_time_s
-- (adjust bounds per track; 30s..180s is a generic guardrail)
select
  l.session_key, l.driver_number, l.lap_number, l.lap_time_s, l.is_pit_out_lap
from {{ ref('int_laps_enriched') }} l
where l.is_clean_lap = true
  and (
        l.lap_time_s is null
     or l.lap_time_s < 30
     or l.lap_time_s > 180
  )
