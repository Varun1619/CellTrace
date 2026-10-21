from datetime import datetime, timedelta

import pandas as pd

from scripts.detection import alert_episodes

T0 = datetime(2026, 1, 5, 8, 0)


def alerts(rows):
    return pd.DataFrame([{"station_id": s, "metric": m, "window_start": T0 + timedelta(minutes=minute),
                          "rule_triggered": rule, "attributed": attributed}
                         for s, m, minute, rule, attributed in rows])


def test_consecutive_windows_on_one_series_are_one_episode():
    eps = alert_episodes(alerts([
        ("L1-S1", "voltage_v", 10, "WE4", False),
        ("L1-S1", "voltage_v", 10, "WE3", False),  # two rules on the same window
        ("L1-S1", "voltage_v", 11, "WE4", False),
        ("L1-S1", "voltage_v", 12, "WE4", False),
        ("L1-S1", "voltage_v", 15, "WE1", False),  # gap: a new episode
        ("L2-S3", "voltage_v", 11, "WE4", False),  # other series
    ]))
    assert len(eps) == 3
    first = eps.iloc[0]
    assert (first.windows, first.alerts, first.rules) == (3, 4, "WE3,WE4")


def test_episode_is_attributed_if_any_of_its_alerts_is():
    eps = alert_episodes(alerts([
        ("L2-S3", "internal_resistance_mohm", 20, "WE1", True),
        ("L2-S3", "internal_resistance_mohm", 21, "WE1", True),
        ("L1-S1", "capacity_ah", 5, "WE3", False),
    ]))
    assert sorted(eps.attributed) == [False, True]


def test_no_alerts_no_episodes():
    assert alert_episodes(alerts([])).empty
