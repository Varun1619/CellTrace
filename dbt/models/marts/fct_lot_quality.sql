-- Capacity and IR statistics per lot, compared against the median lot.
-- z uses the lot's own standard error; effect_sd is the shift in within-lot standard deviations.
-- A lot is an outlier only if both are large, so tiny shifts in big lots and noise in small lots are not flagged.
with lots as (
    select
        lot_id,
        count(*) as n_cells,
        avg(capacity_ah) as capacity_mean,
        stddev_samp(capacity_ah) as capacity_std,
        avg(internal_resistance_mohm) as ir_mean,
        stddev_samp(internal_resistance_mohm) as ir_std,
        avg(case when result = 'FAIL' then 1.0 else 0.0 end) as fail_rate,
        min(tested_at) as first_tested_at,
        max(tested_at) as last_tested_at
    from {{ ref('stg_cell_tests') }}
    group by lot_id
),

reference as (
    select
        median(capacity_mean) as capacity_ref,
        median(capacity_std) as capacity_sd,
        median(ir_mean) as ir_ref,
        median(ir_std) as ir_sd
    from lots
    where n_cells >= 30
),

scored as (
    select
        l.*,
        (l.capacity_mean - r.capacity_ref) / (r.capacity_sd / sqrt(l.n_cells)) as capacity_z,
        (l.capacity_mean - r.capacity_ref) / r.capacity_sd as capacity_effect_sd,
        (l.ir_mean - r.ir_ref) / (r.ir_sd / sqrt(l.n_cells)) as ir_z,
        (l.ir_mean - r.ir_ref) / r.ir_sd as ir_effect_sd
    from lots as l
    cross join reference as r
),

flagged as (
    select
        *,
        abs(capacity_z) > {{ var('lot_outlier_z') }}
            and abs(capacity_effect_sd) >= {{ var('lot_outlier_min_effect_sd') }} as capacity_outlier,
        abs(ir_z) > {{ var('lot_outlier_z') }}
            and abs(ir_effect_sd) >= {{ var('lot_outlier_min_effect_sd') }} as ir_outlier
    from scored
)

select
    *,
    coalesce(capacity_outlier or ir_outlier, false) as is_outlier,
    case
        when capacity_outlier and ir_outlier then 'CAPACITY_AND_IR'
        when capacity_outlier then 'CAPACITY'
        when ir_outlier then 'IR'
    end as outlier_metric
from flagged
