"""Western Electric rules and the per-batch SPC logic. Pure pandas, no Spark."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from streaming.spc_rules import control_limits, evaluate, process_batch

T0 = datetime(2026, 1, 5, 8, 0)
IR = "internal_resistance_mohm"


def windows(means, station="L1-S1", metric=IR, n=250, std=0.05, start=0):
    rows = []
    for i, mean in enumerate(means, start=start):
        ws = T0 + timedelta(minutes=i)
        rows.append({"station_id": station, "metric": metric, "window_start": ws,
                     "window_end": ws + timedelta(minutes=1), "n": n, "mean": mean, "std": std})
    return pd.DataFrame(rows)


EMPTY_HISTORY = pd.DataFrame(columns=["station_id", "metric", "window_start", "window_end", "n", "mean", "std",
                                      "center", "sigma_xbar", "z"])
EMPTY_LIMITS = pd.DataFrame(columns=["station_id", "metric", "center", "sigma", "baseline_windows", "baseline_n"])


# ---- individual rules on z-score sequences ----

@pytest.mark.parametrize("zs, expected", [
    ([0.1, -0.3, 3.4], ["WE1"]),
    ([0.1, -0.2, -3.2], ["WE1"]),
    ([2.3, 0.4, 2.6], ["WE2"]),
    ([1.2, 1.5, 0.2, 1.1, 1.8], ["WE3"]),
    ([0.3, 0.5, 0.2, 0.8, 0.1, 0.4, 0.6, 0.3], ["WE4"]),
    ([-0.3, -0.5, -0.2, -0.8, -0.1, -0.4, -0.6, -0.3], ["WE4"]),
])
def test_each_rule_fires_on_its_pattern(zs, expected):
    assert evaluate(zs) == expected


def test_in_control_sequence_fires_nothing():
    assert evaluate([0.5, -1.1, 0.3, 1.4, -0.2, 0.9, -0.7, 0.1]) == []


def test_rule2_needs_the_same_side():
    assert evaluate([2.5, 0.0, -2.5]) == []


def test_rule2_needs_the_current_point_beyond_two_sigma():
    assert evaluate([2.5, 2.4, 0.5]) == []


def test_large_shift_fires_several_rules():
    assert evaluate([1.5, 1.5, 2.5, 1.2, 3.5]) == ["WE1", "WE2", "WE3"]


# ---- control limits ----

def test_control_limits_use_weighted_center_and_pooled_sigma():
    base = pd.concat([windows([1.18, 1.22], n=100, std=0.04), windows([1.20], n=200, std=0.06, start=2)])
    lim = control_limits(base, baseline_windows=3).iloc[0]
    assert lim.center == pytest.approx((1.18 * 100 + 1.22 * 100 + 1.20 * 200) / 400)
    pooled = np.sqrt((99 * 0.04**2 + 99 * 0.04**2 + 199 * 0.06**2) / (99 + 99 + 199))
    assert lim.sigma == pytest.approx(pooled)
    assert lim.baseline_windows == 3 and lim.baseline_n == 400


def test_control_limits_only_use_the_first_k_windows():
    base = windows([1.2] * 5 + [9.9] * 3)
    assert control_limits(base, baseline_windows=5).iloc[0].center == pytest.approx(1.2)


# ---- batch processing ----

def test_baseline_windows_are_not_scored_and_limits_appear_once_complete():
    out = process_batch(windows([1.2, 1.2, 1.2]), EMPTY_HISTORY, EMPTY_LIMITS, baseline_windows=5, detected_at=T0)
    assert out.new_limits.empty and out.alerts.empty
    assert out.windows["z"].isna().all()

    history = out.windows
    out2 = process_batch(windows([1.2, 1.2, 1.2], start=3), history, EMPTY_LIMITS, baseline_windows=5, detected_at=T0)
    assert len(out2.new_limits) == 1
    scored = out2.windows.dropna(subset=["z"])
    assert list(scored.window_start) == [T0 + timedelta(minutes=5)]


def test_known_drift_fires_rule1_with_value_and_limit():
    sigma_xbar = 0.05 / np.sqrt(250)
    means = [1.2] * 5 + [1.2, 1.2 + 0.5 * sigma_xbar, 1.2 + 4 * sigma_xbar]
    out = process_batch(windows(means), EMPTY_HISTORY, EMPTY_LIMITS, baseline_windows=5, detected_at=T0)
    assert list(out.alerts.rule_triggered) == ["WE1"]
    alert = out.alerts.iloc[0]
    assert alert.window_start == T0 + timedelta(minutes=7)
    assert alert.value == pytest.approx(1.2 + 4 * sigma_xbar)
    assert alert.limit == pytest.approx(1.2 + 3 * sigma_xbar)
    assert alert.station_id == "L1-S1" and alert.metric == IR


def test_slow_drift_is_caught_by_rule4_using_history_across_batches():
    sigma_xbar = 0.05 / np.sqrt(250)
    first = process_batch(windows([1.2] * 5 + [1.2 + 0.5 * sigma_xbar] * 4), EMPTY_HISTORY, EMPTY_LIMITS,
                          baseline_windows=5, detected_at=T0)
    assert first.alerts.empty
    second = process_batch(windows([1.2 + 0.5 * sigma_xbar] * 4, start=9), first.windows, first.new_limits,
                           baseline_windows=5, detected_at=T0)
    assert list(second.alerts.rule_triggered) == ["WE4"]
    assert second.alerts.iloc[0].window_start == T0 + timedelta(minutes=12)
    assert second.alerts.iloc[0].limit == pytest.approx(1.2)


def test_series_are_independent_per_station_and_metric():
    sigma_xbar = 0.05 / np.sqrt(250)
    drifting = windows([1.2] * 5 + [1.2 + 5 * sigma_xbar], station="L2-S3")
    steady = windows([1.2] * 6, station="L1-S1")
    other_metric = windows([5.0] * 6, metric="capacity_ah")
    out = process_batch(pd.concat([drifting, steady, other_metric]), EMPTY_HISTORY, EMPTY_LIMITS,
                        baseline_windows=5, detected_at=T0)
    assert len(out.new_limits) == 3
    assert list(zip(out.alerts.station_id, out.alerts.metric)) == [("L2-S3", IR)]
