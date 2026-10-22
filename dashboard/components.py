"""Small HTML building blocks Streamlit does not provide: KPI tiles, the plant status grid, page styles.

Pure functions returning HTML strings, so they are testable without a browser. Every value
is escaped. Colors come from the same dark palette as charts.py; status colors always sit
next to a text label, never alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from math import isnan

import pandas as pd

from dashboard.charts import BASELINE, CRITICAL, GRID, INK, INK_SECONDARY, MUTED, SURFACE

GOOD = "#0ca30c"
WARNING = "#fab219"
STATUS = {
    "alert": ("Alert", CRITICAL),
    "watch": ("Watch", WARNING),
    "ok": ("In control", GOOD),
    "baseline": ("Baseline", MUTED),
    "missing": ("No data", MUTED),
}
WATCH_Z = 2.0

CSS = f"""
<style>
  [data-testid="stHeader"] {{ background: transparent; }}
  .block-container {{ padding-top: 2.2rem; max-width: 1400px; }}
  .ct-header {{ display: flex; justify-content: space-between; align-items: flex-end; gap: 16px;
               flex-wrap: wrap; margin-bottom: 14px; }}
  .ct-title {{ font-size: 1.9rem; font-weight: 650; color: {INK}; line-height: 1.1; margin: 0; }}
  .ct-sub {{ color: {INK_SECONDARY}; font-size: 0.92rem; margin-top: 4px; }}
  .ct-meta {{ color: {MUTED}; font-size: 0.82rem; text-align: right; line-height: 1.5; }}
  .ct-kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px;
             margin: 6px 0 18px; }}
  .ct-tile {{ background: {SURFACE}; border: 1px solid rgba(255,255,255,0.10); border-radius: 8px;
             padding: 12px 14px; }}
  .ct-tile-label {{ color: {MUTED}; font-size: 0.78rem; }}
  .ct-tile-value {{ color: {INK}; font-size: 1.55rem; font-weight: 600; margin-top: 2px; }}
  .ct-tile-note {{ color: {INK_SECONDARY}; font-size: 0.78rem; margin-top: 2px; }}
  .ct-grid {{ width: 100%; border-collapse: separate; border-spacing: 0; font-size: 0.82rem;
             background: {SURFACE}; border: 1px solid rgba(255,255,255,0.10); border-radius: 8px; overflow: hidden; }}
  .ct-grid th {{ color: {MUTED}; font-weight: 500; text-align: left; padding: 7px 10px;
                border-bottom: 1px solid {BASELINE}; }}
  .ct-grid td {{ padding: 5px 10px; border-bottom: 1px solid {GRID}; color: {INK_SECONDARY};
                white-space: nowrap; }}
  .ct-grid tr:last-child td {{ border-bottom: none; }}
  .ct-grid td.ct-station {{ color: {INK}; font-weight: 600; }}
  .ct-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 7px;
            vertical-align: 1px; }}
  .ct-z {{ color: {MUTED}; margin-left: 6px; font-variant-numeric: tabular-nums; }}
  .ct-section {{ color: {INK}; font-size: 1.02rem; font-weight: 600; margin: 18px 0 6px; }}
  .ct-help {{ color: {MUTED}; font-size: 0.8rem; margin: -2px 0 8px; }}
</style>
"""


@dataclass(frozen=True)
class Tile:
    label: str
    value: str
    note: str | None = None


def _missing(v) -> bool:
    return v is None or (isinstance(v, float) and isnan(v))


def classify(z: float | None, recent_alerts: int) -> str:
    """Status of one station and metric: an alert in the recent windows beats a large z."""
    if recent_alerts and recent_alerts > 0:
        return "alert"
    if _missing(z):
        return "baseline"
    return "watch" if abs(z) > WATCH_Z else "ok"


def header(title: str, subtitle: str, meta_lines: list[str]) -> str:
    meta = "<br>".join(escape(m) for m in meta_lines)
    return (f'<div class="ct-header"><div><p class="ct-title">{escape(title)}</p>'
            f'<div class="ct-sub">{escape(subtitle)}</div></div><div class="ct-meta">{meta}</div></div>')


def kpi_row(tiles: list[Tile]) -> str:
    cells = []
    for t in tiles:
        note = f'<div class="ct-tile-note">{escape(t.note)}</div>' if t.note else ""
        cells.append(f'<div class="ct-tile"><div class="ct-tile-label">{escape(t.label)}</div>'
                     f'<div class="ct-tile-value">{escape(t.value)}</div>{note}</div>')
    return f'<div class="ct-kpis">{"".join(cells)}</div>'


def _status_cell(row) -> str:
    if row is None:
        label, color = STATUS["missing"]
        return f'<td><span class="ct-dot" style="background:{color}"></span>{label}</td>'
    label, color = STATUS[classify(row["z"], row["recent_alerts"])]
    z = "" if _missing(row["z"]) else f'<span class="ct-z">z {row["z"]:+.1f}</span>'
    return f'<td><span class="ct-dot" style="background:{color}"></span>{label}{z}</td>'


def status_grid(status: pd.DataFrame, metrics: dict[str, str]) -> str:
    """One row per station, one column per metric, each cell a status dot, label and latest z."""
    lookup = {(r.station_id, r.metric): {"z": r.z, "recent_alerts": r.recent_alerts}
              for r in status.itertuples()}
    head = "".join(f"<th>{escape(label)}</th>" for label in metrics.values())
    rows = []
    for station in sorted(status.station_id.unique()):
        cells = "".join(_status_cell(lookup.get((station, m))) for m in metrics)
        rows.append(f'<tr><td class="ct-station">{escape(station)}</td>{cells}</tr>')
    return f'<table class="ct-grid"><tr><th>Station</th>{head}</tr>{"".join(rows)}</table>'


def section(title: str, help_text: str | None = None) -> str:
    helper = f'<div class="ct-help">{escape(help_text)}</div>' if help_text else ""
    return f'<div class="ct-section">{escape(title)}</div>{helper}'
