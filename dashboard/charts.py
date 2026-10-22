"""Plotly figures for the dashboard. Pure functions of DataFrames so they can be tested.

Colors follow the dataviz reference palette, dark mode: series take categorical slots in
fixed order (validated all-pairs for three slots), control limits are muted reference
lines, and alerts use the reserved critical status color with an X marker and a label.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

SURFACE = "#1a1a19"
INK = "#f0efec"
INK_SECONDARY = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
BASELINE = "#383835"
SERIES = ["#3987e5", "#d95926", "#199e70"]  # blue, orange, aqua: categorical slots 1-3, dark steps
CRITICAL = "#d03b3b"
CONTEXT = "#55544f"  # de-emphasized bars: one series is the point, the rest are context
# Blue sequential ramp, dark steps first so low values recede into the dark surface.
SEQUENTIAL = ["#0d366b", "#184f95", "#256abf", "#3987e5", "#6da7ec", "#9ec5f4", "#cde2fb"]
Z_CLIP = 6.0
FAILURE_CAP = 0.25  # heatmap color saturates here so a 13% bad lot still stands out next to a 99% drift
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def _layout(fig: go.Figure, title: str, y_title: str, height: int = 380) -> go.Figure:
    fig.update_layout(
        title={"text": title, "font": {"size": 15, "color": INK}, "x": 0, "xanchor": "left"},
        height=height,
        margin={"l": 8, "r": 8, "t": 48, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=SURFACE,
        font={"family": FONT, "color": INK_SECONDARY, "size": 12},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.0, "xanchor": "right", "x": 1,
                "font": {"color": INK_SECONDARY}},
        hoverlabel={"bgcolor": SURFACE, "bordercolor": BASELINE, "font": {"color": INK, "family": FONT}},
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, linecolor=BASELINE, tickfont={"color": MUTED})
    fig.update_yaxes(title={"text": y_title, "font": {"color": MUTED}}, gridcolor=GRID, zeroline=False,
                     linecolor=BASELINE, tickfont={"color": MUTED})
    return fig


def x_range(windows: pd.DataFrame) -> list:
    """Shared time range for the stacked SPC charts, so their windows line up vertically."""
    pad = pd.Timedelta(seconds=30)
    return [windows.window_start.min() - pad, windows.window_start.max() + pad]


def _band(fig: go.Figure, x, y, name: str, dash: str, width: float = 1, show: bool = True) -> None:
    fig.add_trace(go.Scatter(x=x, y=y, name=name, mode="lines", showlegend=show, legendgroup=name,
                             line={"color": MUTED, "width": width, "dash": dash}, hoverinfo="skip"))


def control_chart(windows: pd.DataFrame, alerts: pd.DataFrame, metric_label: str, station: str) -> go.Figure:
    """Window means with the center line, 2 and 3 sigma limits, the baseline shaded and alerts marked."""
    fig = go.Figure()
    if windows.empty:
        return _layout(fig, f"{station}: no closed windows yet", metric_label)
    x = windows.window_start
    ucl = windows.center + 3 * windows.sigma_xbar
    lcl = windows.center - 3 * windows.sigma_xbar
    _band(fig, x, ucl, "3 sigma limits", "dash", 1.5)
    _band(fig, x, lcl, "3 sigma limits", "dash", 1.5, show=False)
    _band(fig, x, windows.center + 2 * windows.sigma_xbar, "2 sigma", "dot")
    _band(fig, x, windows.center - 2 * windows.sigma_xbar, "2 sigma", "dot", show=False)
    _band(fig, x, windows.center, "Center line", "solid")

    baseline = windows[windows.in_baseline]
    if not baseline.empty:
        fig.add_vrect(x0=baseline.window_start.min(), x1=baseline.window_end.max(), fillcolor=MUTED,
                      opacity=0.12, line_width=0, annotation_text="baseline", annotation_position="top left",
                      annotation_font_color=MUTED)

    fig.add_trace(go.Scatter(
        x=x, y=windows["mean"], name="Window mean", mode="lines+markers",
        line={"color": SERIES[0], "width": 2}, marker={"size": 8, "color": SERIES[0],
                                                       "line": {"color": SURFACE, "width": 2}},
        customdata=windows[["z", "n"]].to_numpy(),
        hovertemplate="mean %{y:.4f}<br>z %{customdata[0]:.2f}<br>n %{customdata[1]}<extra></extra>",
    ))
    if not alerts.empty:
        rules = alerts.groupby("window_start").agg(value=("value", "first"),
                                                    rules=("rule_triggered", lambda r: ", ".join(sorted(set(r)))))
        fig.add_trace(go.Scatter(
            x=rules.index, y=rules.value, name="Alert", mode="markers",
            marker={"symbol": "x", "size": 13, "color": CRITICAL, "line": {"color": SURFACE, "width": 1}},
            customdata=rules[["rules"]].to_numpy(), hovertemplate="alert %{customdata[0]}<extra></extra>",
        ))
    fig = _layout(fig, f"{station}: {metric_label}", metric_label)
    fig.update_xaxes(range=x_range(windows))
    return fig


LABEL_GAP_PX = 13


def label_offsets(values: dict[str, float], px_per_unit: float, gap_px: float = LABEL_GAP_PX) -> dict[str, float]:
    """Vertical pixel shifts that keep end-of-line labels at least gap_px apart, nudging upward."""
    shifts, last_px = {}, None
    for name, value in sorted(values.items(), key=lambda kv: kv[1]):
        px = value * px_per_unit
        shifts[name] = 0.0 if last_px is None or px - last_px >= gap_px else (last_px + gap_px) - px
        last_px = px + shifts[name]
    return shifts


def fpy_trend(df: pd.DataFrame, plot_height_px: float = 300) -> go.Figure:
    """First-pass yield per line over time, one fixed color per line, labeled at the last point."""
    fig = go.Figure()
    last = df.sort_values("bucket").groupby("line_id").tail(1).set_index("line_id").first_pass_yield
    span = max(float(df.first_pass_yield.max() - df.first_pass_yield.min()), 0.01)
    shifts = label_offsets(last.to_dict(), plot_height_px / span)
    for i, (line, g) in enumerate(df.groupby("line_id", sort=True)):
        color = SERIES[i % len(SERIES)]
        fig.add_trace(go.Scatter(
            x=g.bucket, y=g.first_pass_yield, name=line, mode="lines", line={"color": color, "width": 2},
            customdata=g[["cells_tested"]].to_numpy(),
            hovertemplate=f"{line} " + "%{y:.1%} of %{customdata[0]} cells<extra></extra>",
        ))
        end = g.iloc[-1]
        fig.add_annotation(x=end.bucket, y=end.first_pass_yield, text=line, showarrow=False, xanchor="left",
                           xshift=6, yshift=shifts[line], font={"color": INK_SECONDARY, "size": 11})
    fig = _layout(fig, "First-pass yield by line, 5-minute buckets", "First-pass yield")
    fig.update_yaxes(tickformat=".0%")
    return fig


def fpy_by_station(df: pd.DataFrame) -> go.Figure:
    """Horizontal bars, worst station in the accent color and the rest as gray context."""
    df = df.sort_values("first_pass_yield", ascending=False)
    worst = df.first_pass_yield.min()
    colors = [SERIES[0] if v == worst else CONTEXT for v in df.first_pass_yield]
    fig = go.Figure(go.Bar(
        x=df.first_pass_yield, y=df.station_id, orientation="h", marker={"color": colors, "cornerradius": 4},
        text=[f"{v:.1%}" for v in df.first_pass_yield], textposition="outside", textfont={"color": INK_SECONDARY},
        customdata=df[["cells_tested"]].to_numpy(),
        hovertemplate="%{y}: %{x:.2%} of %{customdata[0]} cells<extra></extra>",
    ))
    fig = _layout(fig, "First-pass yield by station", "", height=420)
    low = max(0.0, float(df.first_pass_yield.min()) - 0.05)
    fig.update_xaxes(tickformat=".0%", range=[low, 1.0], showgrid=True, gridcolor=GRID)
    fig.update_layout(hovermode="closest", bargap=0.25)
    return fig


def defect_pareto(df: pd.DataFrame) -> go.Figure:
    """Ranked failure causes. Cumulative share is in the hover and labels, not on a second axis."""
    ordered = df.sort_values("failures")
    fig = go.Figure(go.Bar(
        x=ordered.failures, y=ordered.failure_code, orientation="h",
        marker={"color": SERIES[0], "cornerradius": 4},
        text=[f"{s:.0%}" for s in ordered.share], textposition="outside", textfont={"color": INK_SECONDARY},
        customdata=ordered[["share", "cumulative_share"]].to_numpy(),
        hovertemplate="%{y}: %{x} failures<br>%{customdata[0]:.1%} of causes, "
                      "%{customdata[1]:.1%} cumulative<extra></extra>",
    ))
    fig = _layout(fig, "Defect Pareto: failures by cause (dbt)", "", height=320)
    # Headroom so the share label on the longest bar is not clipped.
    fig.update_xaxes(showgrid=True, gridcolor=GRID, range=[0, float(ordered.failures.max()) * 1.18])
    fig.update_layout(hovermode="closest", bargap=0.3)
    return fig


def zscore_chart(windows: pd.DataFrame, alerts: pd.DataFrame) -> go.Figure:
    """The same windows in sigma units with the 1, 2 and 3 sigma zones shaded.

    Points beyond +/-Z_CLIP are pinned to the edge as triangles and keep their true z in the
    hover, so one large drift cannot flatten the in-control detail.
    """
    fig = go.Figure()
    scored = windows.dropna(subset=["z"]) if not windows.empty else windows
    if scored.empty:
        return _layout(fig, "Distance from the center line (sigma)", "z", height=230)
    for k, opacity in ((3, 0.07), (2, 0.07), (1, 0.08)):
        fig.add_hrect(y0=-k, y1=k, fillcolor=MUTED, opacity=opacity, line_width=0)
    for y, dash in ((3, "dash"), (-3, "dash"), (0, "solid")):
        fig.add_hline(y=y, line={"color": MUTED, "width": 1, "dash": dash})
    shown = scored.z.clip(-Z_CLIP, Z_CLIP)
    fig.add_trace(go.Scatter(
        x=scored.window_start, y=shown, name="z-score", mode="lines+markers",
        line={"color": SERIES[0], "width": 2}, marker={"size": 7, "color": SERIES[0]},
        customdata=scored[["z"]].to_numpy(), hovertemplate="z %{customdata[0]:.2f}<extra></extra>",
    ))
    beyond = scored[scored.z.abs() > Z_CLIP]
    if not beyond.empty:
        fig.add_trace(go.Scatter(
            x=beyond.window_start, y=beyond.z.clip(-Z_CLIP, Z_CLIP), name="Beyond the chart", mode="markers",
            marker={"symbol": ["triangle-up" if z > 0 else "triangle-down" for z in beyond.z], "size": 11,
                    "color": CRITICAL, "line": {"color": SURFACE, "width": 1}},
            customdata=beyond[["z"]].to_numpy(), hovertemplate="z %{customdata[0]:.1f} (off scale)<extra></extra>",
        ))
    fig = _layout(fig, f"Distance from the center line (sigma), clipped at +/-{Z_CLIP:.0f}", "z", height=250)
    fig.update_yaxes(range=[-Z_CLIP - 0.6, Z_CLIP + 0.6], tickvals=[-6, -3, 0, 3, 6])
    fig.update_xaxes(range=x_range(windows))
    return fig


def fpy_heatmap(df: pd.DataFrame) -> go.Figure:
    """Failure rate per station and 5-minute bucket: one hue, brighter means more failures."""
    pivot = df.pivot_table(index="station_id", columns="bucket", values="first_pass_yield").sort_index()
    cells = df.pivot_table(index="station_id", columns="bucket", values="cells_tested").reindex_like(pivot)
    failure = (1 - pivot).round(4)
    steps = [i / (len(SEQUENTIAL) - 1) for i in range(len(SEQUENTIAL))]
    fig = go.Figure(go.Heatmap(
        z=failure.to_numpy(), x=list(failure.columns), y=list(failure.index),
        colorscale=[[st, c] for st, c in zip(steps, SEQUENTIAL)], zmin=0,
        zmax=min(max(0.10, float(failure.max().max())), FAILURE_CAP), xgap=2, ygap=2,
        customdata=cells.to_numpy(),
        colorbar={"title": {"text": "Failure rate", "font": {"color": MUTED}},
                  "tickvals": [0, 0.05, 0.10, 0.15, 0.20, 0.25], "ticktext": ["0%", "5%", "10%", "15%", "20%", "25%+"],
                  "tickfont": {"color": MUTED}, "outlinewidth": 0, "thickness": 12},
        hovertemplate="%{y} at %{x|%H:%M}<br>%{z:.1%} failed of %{customdata} cells<extra></extra>",
    ))
    fig = _layout(fig, "Failure rate by station, 5-minute buckets", "", height=420)
    fig.update_layout(hovermode="closest")
    fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
    return fig
