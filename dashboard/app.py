"""CellTrace quality dashboard: streamlit run dashboard/app.py"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import get_settings  # noqa: E402
from dashboard import charts, queries  # noqa: E402

REFRESH_S = 10
PLOT_CONFIG = {"displayModeBar": False}
st.set_page_config(page_title="CellTrace", layout="wide")
settings = get_settings()


@st.cache_data(ttl=REFRESH_S, show_spinner=False)
def cached(name: str, *args):
    return getattr(queries, name)(settings, *args)


def fmt_seconds(v) -> str:
    return "n/a" if v is None or v != v else f"{v:.0f} s"


@st.fragment(run_every=REFRESH_S)
def live_spc() -> None:
    stations = cached("stations")
    if not stations:
        st.info("No SPC windows yet. Start the generator and streams: make pipeline")
        return
    left, mid, right = st.columns([1, 1, 2])
    default = stations.index(settings.generator.drift_station) if settings.generator.drift_station in stations else 0
    station = left.selectbox("Station", stations, index=default, key="spc_station")
    metric = mid.selectbox("Metric", list(queries.SPC_METRIC_LABELS), format_func=queries.SPC_METRIC_LABELS.get,
                           key="spc_metric")
    minutes = right.slider("Minutes shown", 15, 240, 90, step=15, key="spc_minutes")

    windows, alerts = cached("control_chart", station, metric, minutes)
    st.plotly_chart(charts.control_chart(windows, alerts, queries.SPC_METRIC_LABELS[metric], station),
                    use_container_width=True, config=PLOT_CONFIG)

    recent = cached("recent_alerts", 15)
    st.caption(f"Alerting series in the last 15 minutes of event time. Refreshes every {REFRESH_S} s.")
    if recent.empty:
        st.write("No alerts in the last 15 minutes.")
    else:
        st.dataframe(recent, hide_index=True, use_container_width=True)


def yield_tab() -> None:
    trend = cached("fpy_trend", 5)
    if trend.empty:
        st.info("No cell tests in silver yet.")
    else:
        st.plotly_chart(charts.fpy_trend(trend), use_container_width=True, config=PLOT_CONFIG)
    by_station, pareto = cached("fpy_by_station"), cached("defect_pareto")
    if by_station is None or pareto is None:
        st.info("Station FPY and the defect Pareto come from dbt. Run: make dbt")
        return
    left, right = st.columns(2)
    left.plotly_chart(charts.fpy_by_station(by_station), use_container_width=True, config=PLOT_CONFIG)
    right.plotly_chart(charts.defect_pareto(pareto), use_container_width=True, config=PLOT_CONFIG)
    with st.expander("Table view"):
        st.dataframe(pareto, hide_index=True, use_container_width=True)
        st.dataframe(by_station, hide_index=True, use_container_width=True)


def alerts_tab() -> None:
    ttd = cached("time_to_detect")
    st.subheader("Time to detect injected faults")
    if ttd.empty:
        st.info("No faults logged yet. Station drift starts 10 minutes into a generator run.")
    else:
        cols = st.columns(max(len(ttd), 1))
        for col, row in zip(cols, ttd.itertuples()):
            col.metric(f"{row.fault_type} on {row.target}", fmt_seconds(row.time_to_detect_s),
                       help="Fault start to the alert being written (includes pipeline latency)")
            col.caption(f"{fmt_seconds(row.event_time_to_detect_s)} of event time, first rule "
                        f"{row.first_rule if row.first_rule == row.first_rule else 'none yet'}")
    st.subheader("All alerts")
    table = cached("alerts")
    if table.empty:
        st.write("No alerts yet.")
        return
    explained = int(table.attributed.sum())
    st.caption(f"{len(table)} alerts, {explained} explained by an injected fault, "
               f"{len(table) - explained} unexplained (false alarms).")
    st.dataframe(
        table[["detected_at", "station_id", "metric", "rule_triggered", "rule_description", "window_start",
               "value", "limit", "z", "attributed"]],
        hide_index=True, use_container_width=True,
    )


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
    a, b, c, d = st.columns(4)
    a.metric("Packs affected", len(result.packs))
    b.metric("Modules affected", len(result.modules))
    c.metric("Cells in packs", result.counts["in_packs"])
    d.metric("Lookup time", f"{result.elapsed_s * 1000:.0f} ms")
    st.caption(f"{result.counts['cells']} cells tested from this lot: {result.counts['failed_at_test']} failed at "
               f"test and were never assembled, {result.counts['not_yet_assembled']} passed but are not in a "
               "module yet.")
    if result.packs:
        st.write("**Packs:** " + ", ".join(result.packs))
    st.dataframe(result.cells, hide_index=True, use_container_width=True)
    st.download_button("Download affected cells (CSV)", result.cells.to_csv(index=False),
                       file_name=f"recall_{lot_id}.csv", mime="text/csv")


st.title("CellTrace")
st.caption("Battery cell end-of-line quality: SPC drift detection, yield and lot recall")
spc, yld, alr, rcl = st.tabs(["Live SPC", "Yield", "Alerts", "Recall lookup"])
with spc:
    live_spc()
with yld:
    yield_tab()
with alr:
    alerts_tab()
with rcl:
    recall_tab()
