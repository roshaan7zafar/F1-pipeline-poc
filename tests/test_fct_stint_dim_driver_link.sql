-- FAIL if any stint row lacks a matching dim_driver
select s.session_key, s.driver_sk, s.stint_number
from {{ ref('fct_stint') }} s
left join {{ ref('dim_driver') }} d on d.driver_sk = s.driver_sk
where d.driver_sk is null;
