{{ config(
    tags=["slack_test"]
) }}

select distinct organization_id, dt, total_simplified_balance_day_on_day_ratio 
from {{ ref('f_organizations_daily') }} 
where true
and total_simplified_balance_day_on_day_ratio > 1.5 
{# and dt >= '2024-04-01'::date  commented in case we want to run the test only for future dates #}
