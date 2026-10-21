-- One row per violated spec. A cell that breaks two specs appears twice, once per cause.
with readings as (
    unpivot (select * from {{ ref('stg_cell_tests') }})
    on voltage_v, internal_resistance_mohm, capacity_ah, temp_c
    into name metric value reading
)

select
    r.event_id || '|' || s.failure_code as failure_key,
    r.event_id,
    r.cell_id,
    r.lot_id,
    r.line_id,
    r.station_id,
    r.shift,
    r.test_date,
    r.tested_at,
    s.metric,
    r.reading,
    s.failure_code
from readings as r
inner join {{ ref('spec_limits') }} as s on s.metric = r.metric
where (s.lower_limit is not null and r.reading < s.lower_limit)
   or (s.upper_limit is not null and r.reading > s.upper_limit)
