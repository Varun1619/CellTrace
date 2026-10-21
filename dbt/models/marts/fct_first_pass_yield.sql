-- First-pass yield: share of cells that passed on their first test, by day, line, station and shift.
select
    test_date || '|' || station_id || '|' || shift as fpy_key,
    test_date,
    line_id,
    station_id,
    shift,
    count(*) as cells_tested,
    count(*) filter (where result = 'PASS') as cells_passed,
    count(*) filter (where result = 'PASS') / count(*) as first_pass_yield
from {{ ref('stg_cell_tests') }}
where is_first_test
group by test_date, line_id, station_id, shift
