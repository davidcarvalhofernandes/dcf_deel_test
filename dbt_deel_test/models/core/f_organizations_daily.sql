{% set spine_start = "'2019-01-14'::date" %}

{% set invoice_status_list_query %}
select distinct
status
from {{ source('raw_source_data', 'invoices') }}
order by 1
{% endset %}

{% set invoice_status_list = run_query(invoice_status_list_query) %}

-- generate a date spine
with date_spine as (
    {{ dbt_utils.date_spine(
       datepart = 'day',
       start_date = spine_start,
       end_date = "dateadd('day', 1, current_date())"
       )
   }}
)

, invoice_dates as (
    select
        i.*
        , mid.random_date
    from {{ source('raw_source_data','invoices') }} as i
    left join dwh_stg.map_invoice_dates as mid where i.parent_invoice_id = mid.parent_invoice_id
)

, orgs_per_day as (
    select
        o.*
        , ds.date_day as dt
    from date_spine as ds
    left join {{ ref('d_organizations') }} as o on ds.date_day >= o.created_date_ts::date
)

, s01 as (
    select
        opd.dt
        , opd.organization_id
        {% if execute %}
            {%- for invoice_status in invoice_status_list.columns[0].values() -%}
                , round(sum(case when id.status = '{{ invoice_status }}' then id.amount * id.fx_rate else 0 end), 2)
                    as daily_{{ invoice_status }}_amount_usd
            {% endfor -%}
        {% endif %}
        , daily_paid_amount_usd - daily_refunded_amount_usd as daily_simplified_balance_usd
    from orgs_per_day as opd
    left join invoice_dates as id on opd.organization_id = id.organization_id and opd.dt = id.random_date
        {# where opd.organization_id = -1715681882392170942 testing only#}
    group by 1, 2
)

select
    s01.*
    {% if execute %}
        {%- for invoice_status in invoice_status_list.columns[0].values() -%}
            , round(
                sum(s01.daily_{{ invoice_status }}_amount_usd)
                    over (
                        partition by s01.organization_id
                        order by s01.dt rows between unbounded preceding and current row
                    )
                , 2
            ) as total_{{ invoice_status }}_amount_usd
            , round(
                sum(s01.daily_{{ invoice_status }}_amount_usd)
                    over (
                        partition by s01.organization_id
                        order by s01.dt rows between unbounded preceding and 1 preceding
                    )
                , 2
            ) as total_previous_{{ invoice_status }}_amount_usd
        {% endfor -%}
    {% endif %}
    , total_paid_amount_usd - total_refunded_amount_usd as total_simplified_balance_usd
    , total_previous_paid_amount_usd - total_previous_refunded_amount_usd as total_previous_simplified_balance_usd
    , abs(round(div0null(total_simplified_balance_usd, total_previous_simplified_balance_usd), 2))
        as total_simplified_balance_day_on_day_ratio
from s01
