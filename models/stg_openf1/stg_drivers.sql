{{ config(materialized='view') }}

SELECT
    session_key::number         as session_key,
    driver_number::number       as driver_number,
    full_name::string           as full_name,
    name_acronym::string        as name_acronym,
    team_name::string           as team_name,
    team_color::string          as team_color,
    broadcast_name::string      as broadcast_name,
    country_code::string        as country_code
FROM {{source('raw_openf1','OPENF_1_DRIVERS') }}