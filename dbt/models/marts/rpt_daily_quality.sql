-- One row per test day: volume, yield, quarantine, SPC alerts and the top failure cause.
with tests as (
    select test_date, count(*) as cells_tested, avg(case when result = 'PASS' then 1.0 else 0.0 end) as pass_rate
    from {{ ref('stg_cell_tests') }}
    group by test_date
),

fpy as (
    select test_date, sum(cells_passed) / sum(cells_tested) as first_pass_yield
    from {{ ref('fct_first_pass_yield') }}
    group by test_date
),

quarantine as (
    select cast(quarantined_at as date) as test_date, count(*) as rows_quarantined
    from {{ source('silver', 'cell_tests_quarantine') }}
    group by 1
),

alerts as (
    select cast(window_start as date) as test_date, count(*) as spc_alerts,
           count(distinct station_id || metric) as alerting_series
    from {{ source('gold', 'spc_alerts') }}
    group by 1
),

top_cause as (
    select test_date, failure_code as top_failure_code, share as top_failure_share
    from {{ ref('fct_defect_pareto') }}
    where pareto_rank = 1
),

lots as (
    select cast(first_tested_at as date) as test_date, count(*) filter (where is_outlier) as lots_flagged
    from {{ ref('fct_lot_quality') }}
    group by 1
)

select
    t.test_date,
    t.cells_tested,
    f.first_pass_yield,
    coalesce(q.rows_quarantined, 0) as rows_quarantined,
    coalesce(a.spc_alerts, 0) as spc_alerts,
    coalesce(a.alerting_series, 0) as alerting_series,
    c.top_failure_code,
    c.top_failure_share,
    coalesce(l.lots_flagged, 0) as lots_flagged
from tests as t
left join fpy as f using (test_date)
left join quarantine as q using (test_date)
left join alerts as a using (test_date)
left join top_cause as c using (test_date)
left join lots as l using (test_date)
