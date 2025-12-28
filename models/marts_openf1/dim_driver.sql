{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['session_key','driver_number']) }} as driver_sk,
  session_key, driver_number,
  full_name, name_acronym, team_name, team_color
from {{ ref('stg_drivers') }}
