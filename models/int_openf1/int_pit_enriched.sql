{{ config(materialized='view') }}

with pit as (
  select * from {{ ref('stg_pit') }}
)
select
  p.*,
  row_number() over (partition by p.session_key, p.driver_number order by p.pit_time_utc) as stop_seq
from pit p
