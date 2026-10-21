"""How fast SPC caught each injected fault, and which alerts no fault explains.

    python -m scripts.detection

Two clocks are reported per fault:
- event_time_to_detect_s: fault start to the end of the first alerting window. This is
  the statistical delay (how much data the rules needed).
- time_to_detect_s: fault start to when the alert was written. This adds pipeline
  latency: window close, watermark, trigger interval and the three streaming hops.
"""

from __future__ import annotations

from config.settings import Settings, get_settings
from generator.simulator import drift_delta
from scripts.lakehouse import query

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

ATTRIBUTION_SQL = f"""
SELECT a.*,
       EXISTS (
           SELECT 1 FROM {{silver.fault_log}} f
           WHERE {_MATCH}
             AND (f.fault_type <> 'BAD_LOT' OR a.window_start < f.started_at + to_seconds(CAST(? AS DOUBLE)))
       ) AS attributed
FROM {{gold.spc_alerts}} a
ORDER BY a.window_start
"""


def time_to_detect(settings: Settings | None = None):
    return query(TIME_TO_DETECT_SQL, settings)


def alert_attribution(settings: Settings | None = None, lot_duration_s: float | None = None):
    s = settings or get_settings()
    if lot_duration_s is None:
        # A lot takes cells_per_lot / rate seconds to test; allow two extra windows for late events.
        lot_duration_s = s.generator.cells_per_lot / s.generator.rate + 120
    return query(ATTRIBUTION_SQL, s, [lot_duration_s])


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


if __name__ == "__main__":
    main()
