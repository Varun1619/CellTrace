-- One row per cleaned test, with the first test of each cell marked for first-pass yield.
select
    event_id,
    cell_id,
    lot_id,
    line_id,
    station_id,
    shift,
    ts as tested_at,
    cast(ts as date) as test_date,
    voltage_v,
    internal_resistance_mohm,
    capacity_ah,
    temp_c,
    result,
    row_number() over (partition by cell_id order by ts, event_id) = 1 as is_first_test
from {{ source('silver', 'cell_tests') }}
