-- Lot IDs look like LOT-<run tag>-<lot number>.
with injected as (
    select distinct target as lot_id
    from {{ source('silver', 'fault_log') }}
    where fault_type = 'BAD_LOT'
)

select
    t.lot_id,
    split_part(t.lot_id, '-', 2) as run_tag,
    cast(split_part(t.lot_id, '-', 3) as integer) as lot_number,
    min(t.tested_at) as first_tested_at,
    max(t.tested_at) as last_tested_at,
    count(*) as cells_tested,
    bool_or(i.lot_id is not null) as is_injected_bad_lot
from {{ ref('stg_cell_tests') }} as t
left join injected as i on i.lot_id = t.lot_id
group by t.lot_id
