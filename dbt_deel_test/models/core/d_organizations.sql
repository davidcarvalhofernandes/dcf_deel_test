select
    {{ dbt_utils.star( source('raw_source_data', 'organizations'), relation_alias = 'o') }}
    , coalesce(
        try_to_timestamp(o.created_date, 'yyyy-mm-ddthh24:mi:ss.ff3z')
        , try_to_timestamp(o.created_date, 'yyyy-mm-ddthh24:mi:ssz')
    ) as created_date_ts
    , current_timestamp() as load_date
from {{ source('raw_source_data', 'organizations') }} as o
