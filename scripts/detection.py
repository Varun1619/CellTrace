"""How fast SPC caught each injected fault, and which alerts no fault explains.

    python -m scripts.detection

Two clocks are reported per fault:
- event_time_to_detect_s: fault start to the end of the first alerting window. This is
  the statistical delay (how much data the rules needed).
- time_to_detect_s: fault start to when the alert was written. This adds pipeline
  latency: window close, watermark, trigger interval and the three streaming hops.
"""

from __future__ import annotations

import pandas as pd

from config.settings import Settings, get_settings
from generator.simulator import drift_delta
from scripts.lakehouse import query
from streaming.spc_rules import RULE_LOOKBACK

# An alert matches a fault on the same metric after the fault started. Station drift must be
# on the drifting station; a bad lot reaches every station, so any station counts.
_MATCH = """
    a.metric = f.metric AND a.window_end > f.started_at
    AND (f.fault_type <> 'STATION_DRIFT' OR a.station_id = f.target)
"""

TIME_TO_DETECT_SQL = f"""
WITH matched AS (
    SELECT f.fault_id, a.*,
           row_number() OVER (PARTITION BY f.fault_id ORDER BY a.detected_at, a.window_end, a.station_id) AS rn
    FROM {{silver.fault_log}} f JOIN {{gold.spc_alerts}} a ON {_MATCH}
)
SELECT f.fault_type, f.target, f.metric, f.started_at, f.magnitude,
       m.station_id AS first_alert_station, m.rule_triggered AS first_rule,
       m.window_start AS first_alert_window, m.detected_at AS first_detected_at,
       date_diff('millisecond', f.started_at, m.window_end) / 1000.0 AS event_time_to_detect_s,
       date_diff('millisecond', f.started_at, m.detected_at) / 1000.0 AS time_to_detect_s
FROM {{silver.fault_log}} f LEFT JOIN matched m ON m.fault_id = f.fault_id AND m.rn = 1
ORDER BY f.started_at
"""

# Earliest window a rule looked at, so an alert whose lookback overlaps a fault counts as caused by it.
_LOOKBACK_START = "a.window_start - to_minutes(CASE a.rule_triggered {} ELSE 0 END)".format(
    " ".join(f"WHEN '{rule}' THEN {n}" for rule, n in RULE_LOOKBACK.items()))

ATTRIBUTION_SQL = f"""
SELECT a.*,
       EXISTS (
           SELECT 1 FROM {{silver.fault_log}} f
           WHERE {_MATCH}
             AND (f.fault_type <> 'BAD_LOT' OR {_LOOKBACK_START} < f.started_at + to_seconds(CAST(? AS DOUBLE)))
       ) AS attributed
FROM {{gold.spc_alerts}} a
ORDER BY a.window_start
"""


def time_to_detect(settings: Settings | None = None):
    return query(TIME_TO_DETECT_SQL, settings)


def alert_attribution(settings: Settings | None = None, lot_duration_s: float | None = None):
    s = settings or get_settings()
    if lot_duration_s is None:
        # A lot takes cells_per_lot / rate seconds to test, plus one window for the one it ends in.
        # Later late events cannot reach SPC: windows close at the 1-minute watermark.
        lot_duration_s = s.generator.cells_per_lot / s.generator.rate + 60
    return query(ATTRIBUTION_SQL, s, [lot_duration_s])


def alert_episodes(alerts: pd.DataFrame, window: pd.Timedelta = pd.Timedelta(minutes=1)) -> pd.DataFrame:
    """Merge alerts into episodes: runs of consecutive windows on one station and metric.

    WE3 and WE4 keep firing every window while a series stays off-center, so counting
    windows overstates how many distinct problems there were.
    """
    cols = ["station_id", "metric", "start", "end", "windows", "alerts", "rules", "attributed"]
    if alerts.empty:
        return pd.DataFrame(columns=cols)
    episodes = []
    for (station, metric), g in alerts.sort_values("window_start").groupby(["station_id", "metric"], sort=True):
        starts = pd.Series(sorted(g.window_start.unique()))
        episode_of = (starts.diff() > window).cumsum()
        for _, ws in starts.groupby(episode_of.values):
            part = g[g.window_start.isin(ws)]
            episodes.append({
                "station_id": station, "metric": metric, "start": ws.min(), "end": ws.max(),
                "windows": len(ws), "alerts": len(part),
                "rules": ",".join(sorted(set(part.rule_triggered))), "attributed": bool(part.attributed.any()),
            })
    return pd.DataFrame(episodes, columns=cols)


def drift_at_detection(row, settings: Settings) -> float | None:
    """IR offset the generator had applied by the end of the first alerting window."""
    if row.fault_type != "STATION_DRIFT" or row.event_time_to_detect_s != row.event_time_to_detect_s:
        return None
    cfg = settings.generator
    return drift_delta(cfg.drift_start_after_s + row.event_time_to_detect_s, cfg)


def main() -> None:
    settings = get_settings()
    ttd = time_to_detect(settings)
    print("== time to detect")
    for row in ttd.itertuples():
        if row.first_rule is None or row.first_rule != row.first_rule:
            print(f"{row.fault_type} {row.target}: not detected yet")
            continue
        line = (f"{row.fault_type} {row.target} ({row.metric}): first alert {row.first_rule} on "
                f"{row.first_alert_station}, {row.time_to_detect_s:.0f}s after onset "
                f"({row.event_time_to_detect_s:.0f}s of event time)")
        delta = drift_at_detection(row, settings)
        if delta is not None:
            line += f", drift was {delta:.3f} mOhm at detection"
        print(line)

    alerts = alert_attribution(settings)
    print("\n== alerts")
    if alerts.empty:
        print("no alerts yet")
        return
    print(alerts.groupby(["attributed", "metric", "rule_triggered"]).size().rename("alerts").to_string())
    scored = query("SELECT count(*) AS n FROM {gold.spc_window_stats} WHERE z IS NOT NULL", settings)["n"][0]
    false_alarms = int((~alerts.attributed).sum())
    print(f"\nunexplained alerts: {false_alarms} over {scored} scored windows "
          f"({false_alarms / max(scored, 1):.2%} per window)")
    episodes = alert_episodes(alerts)
    unexplained = episodes[~episodes.attributed]
    print(f"episodes: {len(episodes)}, unexplained {len(unexplained)} on "
          f"{unexplained.groupby(['station_id', 'metric']).ngroups} series")


if __name__ == "__main__":
    main()
