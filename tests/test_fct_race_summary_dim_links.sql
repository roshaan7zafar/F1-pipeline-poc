-- FAIL if a fact row is missing any required dimension row
with f as (
  select * from {{ ref('fct_race_summary') }}
)
select f.session_key, f.driver_sk, f.meeting_key
from f
left join {{ ref('dim_driver') }}  d on d.driver_sk   = f.driver_sk
left join {{ ref('dim_meeting') }} m on m.meeting_key = f.meeting_key
left join {{ ref('stg_sessions') }} s on s.session_key = f.session_key  -- use stg if no dim_session
where d.driver_sk is null
   or m.meeting_key is null
   or s.session_key is null;
