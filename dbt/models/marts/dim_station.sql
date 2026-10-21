select
    station_id,
    any_value(line_id) as line_id,
    cast(split_part(station_id, '-S', 2) as integer) as station_number,
    min(tested_at) as first_test_at,
    max(tested_at) as last_test_at,
    count(*) as tests
from {{ ref('stg_cell_tests') }}
group by station_id
