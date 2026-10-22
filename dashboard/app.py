"""CellTrace quality dashboard: streamlit run dashboard/app.py"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import get_settings  # noqa: E402
from dashboard import charts, components, queries  # noqa: E402
from scripts.detection import alert_episodes  # noqa: E402

REFRESH_S = 10
PLOT_CONFIG = {"displayModeBar": False}
METRIC_SHORT = {"internal_resistance_mohm": "IR", "voltage_v": "Voltage", "capacity_ah": "Capacity"}
FAULT_NAMES = {"STATION_DRIFT": "Station drift", "BAD_LOT": "Bad lot"}

st.set_page_config(page_title="CellTrace", layout="wide")
st.markdown(components.CSS, unsafe_allow_html=True)
settings = get_settings()


@st.cache_data(ttl=REFRESH_S, show_spinner=False)
def cached(name: str, *args):
    return getattr(queries, name)(settings, *args)


def fmt_seconds(v) -> str:
    return "n/a" if v is None or v != v else f"{v:.0f} s"


def hhmm(ts) -> str:
    return "" if ts is None or pd.isna(ts) else pd.Timestamp(ts).strftime("%H:%M")


def plot(fig, container=st) -> None:
    container.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG)


# ---- header and KPI row -------------------------------------------------------------------

@st.fragment(run_every=REFRESH_S)
def header_and_kpis() -> None:
    o = cached("overview")
    meta = ["No data yet. Start the pipeline: make pipeline"]
    if o:
        meta = [f"Run {o['run_tag']}, events {hhmm(o['first_event'])} to {hhmm(o['last_event'])} UTC",
                f"Refreshes every {REFRESH_S} s"]
    st.markdown(components.header("CellTrace", "Battery cell end-of-line quality: SPC drift detection, "
                                  "yield and lot recall", meta), unsafe_allow_html=True)
    if not o:
        return
    status = cached("station_status")
    alerting = int((status.recent_alerts > 0).sum()) if not status.empty else 0
    ttd = cached("time_to_detect")
    drift = ttd[ttd.fault_type == "STATION_DRIFT"] if not ttd.empty else ttd
    drift_value, drift_note = "Not injected yet", f"starts at minute {settings.generator.drift_start_after_s / 60:.0f}"
    if not drift.empty:
        d = drift.iloc[0]
        detected = d.first_rule == d.first_rule and d.first_rule is not None
        drift_value = fmt_seconds(d.time_to_detect_s) if detected else "Watching"
        drift_note = f"{d.target}, rule {d.first_rule}" if detected else f"{d.target}, no alert yet"
    total = o["cells_tested"] + o["quarantined"]
    tiles = [
        components.Tile("Cells tested", f"{o['cells_tested']:,}", f"{settings.generator.rate:.0f} cells/s target"),
        components.Tile("First-pass yield", f"{o['first_pass_yield']:.1%}", "share passing their first test"),
        components.Tile("Quarantined rows", f"{o['quarantined']:,}", f"{o['quarantined'] / max(total, 1):.2%} of events"),
        components.Tile("Alerting series", f"{alerting} of {len(status)}", "rule fired in the last 2 windows"),
        components.Tile("Drift time to detect", drift_value, drift_note),
    ]
    st.markdown(components.kpi_row(tiles), unsafe_allow_html=True)


# ---- Live SPC ------------------------------------------------------------------------------

def worst_series(status: pd.DataFrame) -> tuple[str | None, str | None]:
    if status.empty:
        return None, None
    ranked = status.assign(abs_z=status.z.abs().fillna(0)).sort_values(["recent_alerts", "abs_z"], ascending=False)
    top = ranked.iloc[0]
    return top.station_id, top.metric


def alerts_table(df: pd.DataFrame) -> None:
    st.dataframe(
        df.assign(window=df.window_start.map(hhmm), detected=df.detected_at.map(hhmm),
                  metric=df.metric.map(METRIC_SHORT).fillna(df.metric),
                  cause=df.attributed.map({True: "Injected fault", False: "Unexplained"}))
          [["window", "detected", "station_id", "metric", "rule_triggered", "rule_description", "value", "limit",
            "z", "cause"]],
        hide_index=True, use_container_width=True,
        column_config={
            "window": "Window", "detected": "Detected", "station_id": "Station", "metric": "Metric",
            "rule_triggered": "Rule", "rule_description": "What fired",
            "value": st.column_config.NumberColumn("Window mean", format="%.4f"),
            "limit": st.column_config.NumberColumn("Limit", format="%.4f"),
            "z": st.column_config.NumberColumn("z", format="%+.1f"), "cause": "Cause",
        },
    )


@st.fragment(run_every=REFRESH_S)
def live_spc() -> None:
    status = cached("station_status")
    if status.empty:
        st.info("No SPC windows yet. Start the generator and streams: make pipeline")
        return
    grid_col, chart_col = st.columns([5, 7], gap="large")
    grid_col.markdown(components.section(
        "Plant status", "Latest closed 1-minute window. Alert: a Western Electric rule fired in the last 2 "
        "windows. Watch: more than 2 sigma from the center line."), unsafe_allow_html=True)
    grid_col.markdown(components.status_grid(status, METRIC_SHORT), unsafe_allow_html=True)

    station_default, metric_default = worst_series(status)
    stations = sorted(status.station_id.unique())
    with chart_col:
        left, mid, right = st.columns([1, 1, 1.4])
        station = left.selectbox("Station", stations, index=stations.index(station_default), key="spc_station")
        metric_keys = list(queries.SPC_METRIC_LABELS)
        metric = mid.selectbox("Metric", metric_keys, index=metric_keys.index(metric_default),
                               format_func=queries.SPC_METRIC_LABELS.get, key="spc_metric")
        minutes = right.slider("Minutes shown", 15, 240, 90, step=15, key="spc_minutes")
        windows, alerts = cached("control_chart", station, metric, minutes)
        plot(charts.control_chart(windows, alerts, queries.SPC_METRIC_LABELS[metric], station))
        plot(charts.zscore_chart(windows, alerts))

    recent = cached("alerts")
    if not recent.empty:
        st.markdown(components.section("Latest alerts"), unsafe_allow_html=True)
        alerts_table(recent.head(8))


# ---- Yield ---------------------------------------------------------------------------------

def yield_tab() -> None:
    heat = cached("station_fpy", 5)
    if heat.empty:
        st.info("No cell tests in silver yet.")
        return
    plot(charts.fpy_heatmap(heat))
    left, right = st.columns(2)
    plot(charts.fpy_by_station(cached("station_fpy")), left)
    pareto = cached("defect_pareto")
    if pareto is None:
        right.info("The defect Pareto comes from dbt. Run: make dbt")
    else:
        plot(charts.defect_pareto(pareto), right)
    plot(charts.fpy_trend(cached("fpy_trend", 5)))
    with st.expander("Table view"):
        st.dataframe(cached("station_fpy"), hide_index=True, use_container_width=True)
        if pareto is not None:
            st.dataframe(pareto, hide_index=True, use_container_width=True)


# ---- Alerts --------------------------------------------------------------------------------

def detection_tiles(ttd: pd.DataFrame) -> list[components.Tile]:
    tiles = []
    for r in ttd.itertuples():
        name = f"{FAULT_NAMES.get(r.fault_type, r.fault_type)} on {r.target}"
        if r.first_rule is None or r.first_rule != r.first_rule:
            tiles.append(components.Tile(name, "Watching", f"started {hhmm(r.started_at)}, no alert yet"))
            continue
        tiles.append(components.Tile(name, fmt_seconds(r.time_to_detect_s),
                                     f"{fmt_seconds(r.event_time_to_detect_s)} of event time, {r.first_rule} on "
                                     f"{r.first_alert_station}"))
    return tiles


def alerts_tab() -> None:
    ttd = cached("time_to_detect")
    st.markdown(components.section(
        "Time to detect injected faults", "From the moment the generator started the fault to the alert being "
        "written. Event time is the statistical part; the rest is pipeline latency."), unsafe_allow_html=True)
    if ttd.empty:
        st.info("No faults logged yet.")
    else:
        st.markdown(components.kpi_row(detection_tiles(ttd)), unsafe_allow_html=True)

    table = cached("alerts")
    if table.empty:
        st.write("No alerts yet.")
        return
    left, right = st.columns([2, 1])
    metrics = left.multiselect("Metric", list(METRIC_SHORT), default=list(METRIC_SHORT),
                               format_func=METRIC_SHORT.get)
    cause = right.radio("Cause", ["All", "Injected fault", "Unexplained"], horizontal=True)
    shown = table[table.metric.isin(metrics)]
    if cause != "All":
        shown = shown[shown.attributed == (cause == "Injected fault")]

    episodes = alert_episodes(shown)
    unexplained = episodes[~episodes.attributed] if not episodes.empty else episodes
    st.markdown(components.section(
        "Episodes", f"{len(episodes)} episodes from {len(shown)} alerts; {len(unexplained)} unexplained. An episode "
        "is a run of consecutive alerting windows on one series."), unsafe_allow_html=True)
    if not episodes.empty:
        st.dataframe(
            episodes.assign(start=episodes.start.map(hhmm), end=episodes.end.map(hhmm),
                            metric=episodes.metric.map(METRIC_SHORT),
                            cause=episodes.attributed.map({True: "Injected fault", False: "Unexplained"}))
                    .sort_values(["start", "station_id"])
                    [["start", "end", "station_id", "metric", "windows", "alerts", "rules", "cause"]],
            hide_index=True, use_container_width=True,
            column_config={"start": "From", "end": "To", "station_id": "Station", "metric": "Metric",
                           "windows": "Windows", "alerts": "Alerts", "rules": "Rules", "cause": "Cause"},
        )
    with st.expander(f"All {len(shown)} alerts"):
        alerts_table(shown)


# ---- Recall --------------------------------------------------------------------------------

def recall_tab() -> None:
    lots = cached("lot_ids")
    if not lots:
        st.info("No lots tested yet.")
        return
    quality = cached("lot_quality")
    flagged = set(quality[quality.is_outlier].lot_id) if quality is not None else set()
    default = next((i for i, lot in enumerate(lots) if lot in flagged), 0)
    lot_id = st.selectbox("Lot", lots, index=default,
                          format_func=lambda lot: f"{lot}  (flagged outlier)" if lot in flagged else lot)
    result = queries.recall(settings, lot_id)
    c = result.counts
    st.markdown(components.kpi_row([
        components.Tile("Packs affected", str(len(result.packs)), "contain at least one cell from the lot"),
        components.Tile("Modules affected", str(len(result.modules))),
        components.Tile("Cells inside packs", f"{c['in_packs']:,}", f"of {c['cells']:,} tested from this lot"),
        components.Tile("Failed at test", f"{c['failed_at_test']:,}", "never assembled"),
        components.Tile("Lookup time", f"{result.elapsed_s * 1000:.0f} ms", "DuckDB over Delta"),
    ]), unsafe_allow_html=True)

    packs = queries.pack_summary(result.cells)
    left, right = st.columns([1, 2])
    left.markdown(components.section("Affected packs"), unsafe_allow_html=True)
    left.dataframe(
        packs, hide_index=True, use_container_width=True,
        column_config={
            "pack_id": "Pack", "line_id": "Line", "modules": "Modules",
            "cells_from_lot": st.column_config.ProgressColumn(
                "Cells from lot", format="%d", min_value=0, max_value=int(packs.cells_from_lot.max() or 1)),
        },
    )
    right.markdown(components.section("Cells from this lot"), unsafe_allow_html=True)
    right.dataframe(
        result.cells.assign(tested=result.cells.tested_at.map(hhmm))
        [["cell_id", "result", "station_id", "tested", "capacity_ah", "internal_resistance_mohm", "voltage_v",
          "module_id", "pack_id", "module_history"]],
        hide_index=True, use_container_width=True,
        column_config={
            "cell_id": "Cell", "result": "Result", "station_id": "Station", "tested": "Tested",
            "capacity_ah": st.column_config.NumberColumn("Capacity (Ah)", format="%.3f"),
            "internal_resistance_mohm": st.column_config.NumberColumn("IR (mOhm)", format="%.3f"),
            "voltage_v": st.column_config.NumberColumn("Voltage (V)", format="%.3f"),
            "module_id": "Module", "pack_id": "Pack", "module_history": "Module history",
        },
    )
    right.download_button("Download affected cells (CSV)", result.cells.to_csv(index=False),
                          file_name=f"recall_{lot_id}.csv", mime="text/csv")


header_and_kpis()
spc, yld, alr, rcl = st.tabs(["Live SPC", "Yield", "Alerts", "Recall lookup"])
with spc:
    live_spc()
with yld:
    yield_tab()
with alr:
    alerts_tab()
with rcl:
    recall_tab()
