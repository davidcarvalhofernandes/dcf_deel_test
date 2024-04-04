with min_invoice_dt as (
    select
        o.created_date_ts
        , i.parent_invoice_id
    from {{ source('raw_source_data','invoices') }} as i
    left join {{ ref('d_organizations') }} as o on i.organization_id = o.organization_id
    qualify row_number() over (partition by i.parent_invoice_id order by o.created_date_ts) = 1
)

select
    parent_invoice_id
    , created_date_ts::date as invoice_date
    , dateadd(
        'day'
        , floor(1 + uniform(1, 100, random()))
        , created_date_ts::date
    ) as random_date
from min_invoice_dt
