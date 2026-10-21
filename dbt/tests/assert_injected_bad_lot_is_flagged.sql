-- The generator logs the lot it degraded. Once at least 1,000 of its cells are tested,
-- fct_lot_quality must flag it as a capacity outlier.
select d.lot_id, q.n_cells, q.capacity_z, q.capacity_effect_sd
from {{ ref('dim_lot') }} as d
inner join {{ ref('fct_lot_quality') }} as q using (lot_id)
where d.is_injected_bad_lot
  and q.n_cells >= 1000
  and not coalesce(q.capacity_outlier, false)
