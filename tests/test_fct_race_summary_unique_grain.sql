-- Grain should be one row per (session_key, driver_sk)
select session_key, driver_sk, count(*) as cnt
from {{ ref('fct_race_summary') }}
group by 1,2
having count(*) > 1
