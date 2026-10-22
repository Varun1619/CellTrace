from datetime import datetime, timedelta

import pandas as pd
import pytest

pytest.importorskip("plotly")

from dashboard import charts  # noqa: E402

T0 = datetime(2026, 1, 5, 8, 0)


def windows(n=10, baseline=5):
    rows = []
    for i in range(n):
        ws = T0 + timedelta(minutes=i)
        rows.append({"window_start": ws, "window_end": ws + timedelta(minutes=1), "n": 250,
                     "mean": 1.2 + (0.02 if i == n - 1 else 0.0), "z": None if i < baseline else 0.1,
                     "center": 1.2, "sigma_xbar": 0.003, "in_baseline": i < baseline})
    return pd.DataFrame(rows)


def test_control_chart_has_limits_mean_and_alert_markers():
    alerts = pd.DataFrame([{"window_start": T0 + timedelta(minutes=9), "value": 1.22, "z": 6.7,
                            "rule_triggered": "WE1"}, {"window_start": T0 + timedelta(minutes=9), "value": 1.22,
                                                       "z": 6.7, "rule_triggered": "WE2"}])
    fig = charts.control_chart(windows(), alerts, "IR (mOhm)", "L2-S3")
    names = [t.name for t in fig.data]
    assert names.count("3 sigma limits") == 2 and "Center line" in names and "Window mean" in names
    alert = next(t for t in fig.data if t.name == "Alert")
    assert alert.marker.color == charts.CRITICAL and alert.marker.symbol == "x"
    assert list(alert.customdata[:, 0]) == ["WE1, WE2"]
    ucl = next(t for t in fig.data if t.name == "3 sigma limits")
    assert ucl.y[0] == pytest.approx(1.209)
    assert fig.layout.shapes, "baseline region is shaded"


def test_control_chart_without_windows_says_so():
    fig = charts.control_chart(pd.DataFrame(), pd.DataFrame(), "IR", "L1-S1")
    assert "no closed windows" in fig.layout.title.text


def test_fpy_trend_uses_fixed_colors_in_line_order():
    df = pd.DataFrame([{"bucket": T0 + timedelta(minutes=5 * i), "line_id": line, "cells_tested": 100,
                        "first_pass_yield": 0.97} for i in range(3) for line in ("L3", "L1", "L2")])
    fig = charts.fpy_trend(df)
    assert [t.name for t in fig.data] == ["L1", "L2", "L3"]
    assert [t.line.color for t in fig.data] == charts.SERIES
    assert len(fig.layout.annotations) == 3  # direct labels


def test_pareto_is_single_axis_ranked_bars():
    df = pd.DataFrame({"failure_code": ["CAPACITY_LOW", "IR_HIGH"], "failures": [90, 10], "share": [0.9, 0.1],
                       "cumulative_share": [0.9, 1.0]})
    fig = charts.defect_pareto(df)
    assert len(fig.data) == 1 and fig.data[0].type == "bar"
    assert list(fig.data[0].y) == ["IR_HIGH", "CAPACITY_LOW"]  # largest bar on top
    assert "yaxis2" not in fig.layout.to_plotly_json()


def test_label_offsets_separate_close_labels_and_leave_distant_ones():
    shifts = charts.label_offsets({"L1": 0.970, "L3": 0.971, "L2": 0.740}, px_per_unit=1000)
    assert shifts["L2"] == 0 and shifts["L1"] == 0
    assert shifts["L3"] >= charts.LABEL_GAP_PX - 1  # nudged up past L1


def test_pareto_has_headroom_for_its_labels():
    df = pd.DataFrame({"failure_code": ["A", "B"], "failures": [100, 10], "share": [0.9, 0.1],
                       "cumulative_share": [0.9, 1.0]})
    assert charts.defect_pareto(df).layout.xaxis.range[1] > 100


def test_zscore_chart_clips_at_six_sigma_and_keeps_true_z_in_hover():
    w = windows(n=8, baseline=2)
    w.loc[7, "z"] = 42.0
    fig = charts.zscore_chart(w, pd.DataFrame(columns=["window_start", "rule_triggered"]))
    z_trace = next(t for t in fig.data if t.name == "z-score")
    assert max(z_trace.y) == charts.Z_CLIP
    clipped = next(t for t in fig.data if t.name == "Beyond the chart")
    assert list(clipped.customdata[:, 0]) == [42.0]
    assert fig.layout.yaxis.range[1] >= charts.Z_CLIP


def test_fpy_heatmap_puts_stations_on_rows_and_uses_one_hue():
    rows = [{"bucket": T0 + timedelta(minutes=5 * i), "station_id": s, "cells_tested": 100,
             "first_pass_yield": 0.97 - (0.3 if s == "L2-S3" and i == 2 else 0)}
            for i in range(3) for s in ("L1-S1", "L2-S3")]
    fig = charts.fpy_heatmap(pd.DataFrame(rows))
    hm = fig.data[0]
    assert hm.type == "heatmap" and list(hm.y) == ["L1-S1", "L2-S3"]
    assert hm.z[1][2] == pytest.approx(0.33)  # failure rate, worst cell
    assert len({c[1] for c in hm.colorscale}) == len(hm.colorscale)  # distinct steps of one ramp
    assert hm.zmax == charts.FAILURE_CAP  # 33% saturates; hover keeps the true value


def test_station_bars_emphasize_the_worst_station():
    df = pd.DataFrame({"station_id": ["L1-S1", "L2-S3", "L3-S2"], "line_id": ["L1", "L2", "L3"],
                       "cells_tested": [100, 100, 100], "first_pass_yield": [0.97, 0.71, 0.96]})
    fig = charts.fpy_by_station(df)
    colors = dict(zip(fig.data[0].y, fig.data[0].marker.color))
    assert colors["L2-S3"] == charts.SERIES[0]
    assert colors["L1-S1"] == colors["L3-S2"] == charts.CONTEXT
