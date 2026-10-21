"""Delta maintenance (OPTIMIZE + VACUUM) and the daily report publish, using delta-rs only."""

from datetime import date

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")


def test_optimize_compacts_small_files_without_changing_data(lake):
    from scripts.maintain_delta import maintain_table

    path = lake.table_path("silver", "cell_tests")
    for i in range(6):
        deltalake.write_deltalake(path, pd.DataFrame({"event_id": [f"e{i}"], "v": [float(i)]}), mode="append")
    before = deltalake.DeltaTable(path)
    assert len(before.files()) == 6

    stats = maintain_table(path, vacuum_retention_hours=168)
    after = deltalake.DeltaTable(path)
    assert len(after.files()) == 1
    assert stats["files_removed"] == 6 and stats["files_added"] == 1
    assert sorted(after.to_pandas().event_id) == [f"e{i}" for i in range(6)]


def test_maintain_layer_skips_missing_tables(lake):
    from scripts.maintain_delta import maintain_layer

    deltalake.write_deltalake(lake.table_path("silver", "fault_log"), pd.DataFrame({"x": [1]}))
    results = maintain_layer(lake, "silver", vacuum_retention_hours=168)
    assert set(results) == {"fault_log"}


def test_publish_daily_report_upserts_by_date(lake, tmp_path):
    from scripts.publish_daily_report import publish

    report = tmp_path / "rpt.parquet"
    pd.DataFrame({"test_date": [date(2026, 1, 5), date(2026, 1, 6)], "cells_tested": [100, 200],
                  "first_pass_yield": [0.97, 0.95]}).to_parquet(report)
    assert publish(lake, report) == 2

    pd.DataFrame({"test_date": [date(2026, 1, 6), date(2026, 1, 7)], "cells_tested": [250, 300],
                  "first_pass_yield": [0.96, 0.98]}).to_parquet(report)
    assert publish(lake, report) == 2

    out = deltalake.DeltaTable(lake.table_path("gold", "daily_quality_report")).to_pandas().sort_values("test_date")
    assert list(out.cells_tested) == [100, 250, 300]
    assert out.published_at.notna().all()


def test_publish_single_day(lake, tmp_path):
    from scripts.publish_daily_report import publish

    report = tmp_path / "rpt.parquet"
    pd.DataFrame({"test_date": [date(2026, 1, 5), date(2026, 1, 6)], "cells_tested": [100, 200]}).to_parquet(report)
    assert publish(lake, report, only_date=date(2026, 1, 6)) == 1
    out = deltalake.DeltaTable(lake.table_path("gold", "daily_quality_report")).to_pandas()
    assert list(out.cells_tested) == [200]
