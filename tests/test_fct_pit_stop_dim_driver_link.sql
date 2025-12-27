-- FAIL if any pit row lacks a matching dim_driver
select p.session_key, p.driver_sk, p.lap_number, p.pit_time_utc
from {{ ref('fct_pit_stop') }} p
left join {{ ref('dim_driver') }} d on d.driver_sk = p.driver_sk
where d.driver_sk is null;
