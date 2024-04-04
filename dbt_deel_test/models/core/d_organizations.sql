select
    {{ dbt_utils.star( source('raw_source_data', 'organizations'), relation_alias = 'o') }}
    , coalesce(
        try_to_timestamp(o.created_date, 'YYYY-MM-DDTHH24:MI:SS.FF3Z')
        , try_to_timestamp(o.created_date, 'YYYY-MM-DDTHH24:MI:SSZ')
    ) as created_date_ts
    , current_timestamp() as load_date
from {{ source('raw_source_data', 'organizations') }} as o
