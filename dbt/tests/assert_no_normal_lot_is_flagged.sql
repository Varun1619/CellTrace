-- Only the injected lot should be flagged. A false flag means the outlier thresholds are too loose.
select d.lot_id, q.n_cells, q.capacity_z, q.ir_z
from {{ ref('dim_lot') }} as d
inner join {{ ref('fct_lot_quality') }} as q using (lot_id)
where not d.is_injected_bad_lot and q.is_outlier
