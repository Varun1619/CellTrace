import pandas as pd

from dashboard.components import Tile, classify, kpi_row, status_grid


def test_classify_status_from_z_and_recent_alerts():
    assert classify(z=None, recent_alerts=0) == "baseline"
    assert classify(z=0.4, recent_alerts=0) == "ok"
    assert classify(z=-2.6, recent_alerts=0) == "watch"
    assert classify(z=0.1, recent_alerts=2) == "alert"


def test_kpi_row_renders_label_value_and_note_escaped():
    html = kpi_row([Tile("Cells tested", "113,997", "since 22:25"), Tile("<b>x</b>", "1", None)])
    assert "Cells tested" in html and "113,997" in html and "since 22:25" in html
    assert "<b>x</b>" not in html and "&lt;b&gt;x&lt;/b&gt;" in html


def test_status_grid_has_a_cell_per_station_and_metric_with_label():
    df = pd.DataFrame([
        {"station_id": "L1-S1", "metric": "voltage_v", "z": 0.2, "recent_alerts": 0},
        {"station_id": "L1-S1", "metric": "capacity_ah", "z": 2.5, "recent_alerts": 0},
        {"station_id": "L2-S3", "metric": "internal_resistance_mohm", "z": 9.1, "recent_alerts": 3},
    ])
    html = status_grid(df, metrics={"internal_resistance_mohm": "IR", "voltage_v": "Voltage", "capacity_ah": "Capacity"})
    assert html.count("<tr") == 3  # header + two stations
    assert "Alert" in html and "Watch" in html and "In control" in html
    assert "No data" in html  # L1-S1 IR and L2-S3 voltage/capacity are missing
    assert "9.1" in html
