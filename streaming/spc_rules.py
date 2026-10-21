"""Statistical process control on 1-minute subgroups (X-bar chart with variable n).

Pure pandas so the rules can be tested without Spark. spc_monitor.py feeds this
module closed windows from the stream and writes back what it returns.

Each station and metric is its own series. The first `baseline_windows` windows
set the control limits: center is the n-weighted grand mean and sigma is the pooled
within-window standard deviation. Later windows are scored as
z = (mean - center) / (sigma / sqrt(n)) and checked against the Western Electric rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import numpy as np
import pandas as pd

SPC_METRICS = ("internal_resistance_mohm", "voltage_v", "capacity_ah")
RULES = {
    "WE1": "1 point beyond 3 sigma",
    "WE2": "2 of 3 consecutive beyond 2 sigma, same side",
    "WE3": "4 of 5 consecutive beyond 1 sigma, same side",
    "WE4": "8 consecutive on one side of the center line",
}
# Sigma multiple of the limit each rule tests against (WE4 tests the center line).
RULE_SIGMA = {"WE1": 3, "WE2": 2, "WE3": 1, "WE4": 0}
HISTORY_NEEDED = 7
KEY = ["station_id", "metric"]
WINDOW_COLS = ["station_id", "metric", "window_start", "window_end", "n", "mean", "std", "center", "sigma_xbar", "z"]
LIMIT_COLS = ["station_id", "metric", "center", "sigma", "baseline_windows", "baseline_n"]
ALERT_COLS = ["station_id", "metric", "rule_triggered", "rule_description", "window_start", "window_end",
              "value", "limit", "z", "detected_at"]


@dataclass(frozen=True)
class SpcBatchResult:
    windows: pd.DataFrame      # every input window, scored when limits exist (z is NaN during baseline)
    new_limits: pd.DataFrame   # limits computed in this batch
    alerts: pd.DataFrame


def _fires(zs: Sequence[float], sign: int) -> list[str]:
    cur = sign * zs[-1]
    fired = []
    if cur > 2 and sum(sign * z > 2 for z in zs[-3:]) >= 2:
        fired.append("WE2")
    if cur > 1 and sum(sign * z > 1 for z in zs[-5:]) >= 4:
        fired.append("WE3")
    if len(zs) >= 8 and all(sign * z > 0 for z in zs[-8:]):
        fired.append("WE4")
    return fired


def evaluate(zs: Sequence[float]) -> list[str]:
    """Rules that fire at the last point of a z-score series (older points first)."""
    if not zs:
        return []
    fired = ["WE1"] if abs(zs[-1]) > 3 else []
    for sign in (1, -1):
        fired += _fires(zs, sign)
    return sorted(set(fired), key=list(RULES).index)


def control_limits(baseline: pd.DataFrame, baseline_windows: int) -> pd.DataFrame:
    """Limits from the first `baseline_windows` windows of each series."""
    rows = []
    for (station, metric), g in baseline.sort_values("window_start").groupby(KEY, sort=False):
        g = g.head(baseline_windows)
        dof = (g.n - 1).clip(lower=0)
        if len(g) < baseline_windows or dof.sum() == 0:
            continue
        pooled_var = float((dof * g["std"].fillna(0) ** 2).sum() / dof.sum())
        rows.append({
            "station_id": station, "metric": metric,
            "center": float((g.n * g["mean"]).sum() / g.n.sum()),
            "sigma": float(np.sqrt(pooled_var)),
            "baseline_windows": len(g), "baseline_n": int(g.n.sum()),
        })
    return pd.DataFrame(rows, columns=LIMIT_COLS)


def concat_nonempty(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """pd.concat that skips empty frames (pandas warns about their dtypes otherwise)."""
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def _limits_for_unbaselined(new: pd.DataFrame, history: pd.DataFrame, limits: pd.DataFrame,
                            baseline_windows: int) -> tuple[pd.DataFrame, set]:
    """Compute limits for series that just reached the baseline size. Returns limits and baseline window keys."""
    have = set(map(tuple, limits[KEY].values)) if not limits.empty else set()
    pending = concat_nonempty([history[history.z.isna()][new.columns.intersection(history.columns)], new], list(new.columns))
    pending = pending[[k not in have for k in map(tuple, pending[KEY].values)]]
    fresh = control_limits(pending, baseline_windows) if not pending.empty else pd.DataFrame(columns=LIMIT_COLS)
    used = set()
    for _, lim in fresh.iterrows():
        series = pending[(pending.station_id == lim.station_id) & (pending.metric == lim.metric)]
        used |= {(lim.station_id, lim.metric, ws) for ws in series.sort_values("window_start").window_start.head(baseline_windows)}
    return fresh, used


def _alert(row: pd.Series, rule: str, detected_at: datetime) -> dict:
    side = 1 if row.z >= 0 else -1
    return {
        "station_id": row.station_id, "metric": row.metric, "rule_triggered": rule, "rule_description": RULES[rule],
        "window_start": row.window_start, "window_end": row.window_end, "value": float(row["mean"]),
        "limit": float(row.center + side * RULE_SIGMA[rule] * row.sigma_xbar), "z": float(row.z),
        "detected_at": detected_at,
    }


def process_batch(new: pd.DataFrame, history: pd.DataFrame, limits: pd.DataFrame, baseline_windows: int,
                  detected_at: datetime) -> SpcBatchResult:
    """Score newly closed windows against limits and history, creating limits once a baseline is complete."""
    fresh, baseline_keys = _limits_for_unbaselined(new, history, limits, baseline_windows)
    all_limits = concat_nonempty([limits[LIMIT_COLS], fresh], LIMIT_COLS)
    scored = new.merge(all_limits[["station_id", "metric", "center", "sigma"]], on=KEY, how="left")
    in_baseline = [(r.station_id, r.metric, r.window_start) in baseline_keys for r in scored.itertuples()]
    scored.loc[in_baseline, ["center", "sigma"]] = np.nan
    scored["sigma_xbar"] = scored.sigma / np.sqrt(scored.n)
    scored["z"] = (scored["mean"] - scored.center) / scored.sigma_xbar

    prior = history.dropna(subset=["z"]).sort_values("window_start")
    alerts = []
    for (station, metric), g in scored.dropna(subset=["z"]).sort_values("window_start").groupby(KEY, sort=False):
        zs = list(prior[(prior.station_id == station) & (prior.metric == metric)].z.tail(HISTORY_NEEDED))
        for _, row in g.iterrows():
            zs.append(row.z)
            alerts += [_alert(row, rule, detected_at) for rule in evaluate(zs)]
    return SpcBatchResult(
        windows=scored[WINDOW_COLS].reset_index(drop=True),
        new_limits=fresh.reset_index(drop=True),
        alerts=pd.DataFrame(alerts, columns=ALERT_COLS),
    )
