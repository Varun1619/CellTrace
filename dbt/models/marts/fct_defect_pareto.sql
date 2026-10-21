-- Failure causes per day, ranked, with each cause's share and the running cumulative share.
with counts as (
    select test_date, failure_code, count(*) as failures
    from {{ ref('stg_cell_failures') }}
    group by test_date, failure_code
)

select
    test_date || '|' || failure_code as pareto_key,
    test_date,
    failure_code,
    failures,
    rank() over (partition by test_date order by failures desc, failure_code) as pareto_rank,
    failures / sum(failures) over (partition by test_date) as share,
    sum(failures) over (
        partition by test_date order by failures desc, failure_code
        rows between unbounded preceding and current row
    ) / sum(failures) over (partition by test_date) as cumulative_share
from counts
