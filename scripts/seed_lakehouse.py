"""Build a complete lakehouse in seconds, without Kafka or Spark, from the same pure cores.

    python -m scripts.seed_lakehouse --minutes 40

Used by CI (dbt needs real tables to test) and for trying dbt or the dashboard without
starting the streams. It simulates `minutes` of production on a simulated clock, applies
the silver rules (dedupe, quarantine), the SCD2 genealogy and the SPC engine, and
writes the Delta tables with delta-rs. Seeded alerts have no pipeline latency:
detected_at is the end of the alerting window. Run `make reset` before starting the
streams on a seeded lakehouse.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

import pandas as pd
from deltalake import write_deltalake

from config.settings import Settings, get_settings
from config.specs import METRICS, PLAUSIBLE_RANGES
from generator.simulator import run_simulation
from streaming.genealogy_rules import GENEALOGY_COLS, apply_assembly_events
from streaming.spc_rules import LIMIT_COLS, SPC_METRICS, concat_nonempty, process_batch

START = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)
EMPTY_WINDOWS = ["station_id", "metric", "window_start", "window_end", "n", "mean", "std", "center",
                 "sigma_xbar", "z"]


def _utc(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Timezone-aware UTC timestamps, so delta-rs writes the same type Spark does.

    All-null columns become strings; Delta has no null type.
    """
    out = df.copy()
    for c in cols:
        out[c] = pd.to_datetime(out[c], utc=True)
    for c in out.columns:
        if c not in cols and out[c].isna().all():
            out[c] = out[c].astype("string")
    return out


def _write(settings: Settings, layer: str, name: str, df: pd.DataFrame, ts_cols: list[str]) -> None:
    write_deltalake(settings.table_path(layer, name), _utc(df, ts_cols), mode="overwrite", schema_mode="overwrite")


def _reason(row: pd.Series) -> str | None:
    reasons = []
    for m in METRICS:
        low, high = PLAUSIBLE_RANGES[m]
        if pd.isna(row[m]):
            reasons.append(f"null_{m}")
        elif not low <= row[m] <= high:
            reasons.append(f"out_of_range_{m}")
    return ";".join(reasons) or None


def _kafka_meta(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(kafka_partition=0, kafka_offset=range(len(df)), ingest_ts=df.ts)


def _silver_cell_tests(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    tests = _kafka_meta(raw.drop_duplicates("event_id").reset_index(drop=True))
    tests["ts"] = pd.to_datetime(tests.ts, utc=True)
    tests["ingest_ts"] = tests.ts
    tests["quarantine_reason"] = tests.apply(_reason, axis=1)
    bad = tests[tests.quarantine_reason.notna()]
    quarantine = pd.DataFrame({
        "quarantine_reason": bad.quarantine_reason, "event_id": bad.event_id, "station_id": bad.station_id,
        "ts_raw": bad.ts.astype(str), "payload": None, "kafka_partition": bad.kafka_partition,
        "kafka_offset": bad.kafka_offset, "ingest_ts": bad.ts, "quarantined_at": bad.ts,
    })
    clean = tests[tests.quarantine_reason.isna()].drop(columns="quarantine_reason")
    clean = clean.assign(test_date=clean.ts.dt.date)
    return clean, quarantine


def _spc(settings: Settings, clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Feed closed windows to the SPC engine one minute at a time, the way the stream does."""
    df = clean.assign(window_start=clean.ts.dt.floor("1min").dt.tz_localize(None))
    agg = df.groupby(["station_id", "window_start"])[list(SPC_METRICS)].agg(["count", "mean", "std"])
    long = []
    for m in SPC_METRICS:
        part = agg[m].reset_index().rename(columns={"count": "n"})
        long.append(part.assign(metric=m, window_end=part.window_start + pd.Timedelta(minutes=1)))
    windows = pd.concat(long, ignore_index=True)
    history = pd.DataFrame(columns=EMPTY_WINDOWS)
    limits = pd.DataFrame(columns=LIMIT_COLS)
    scored, alerts, new_limits = [], [], []
    for ws, batch in windows.groupby("window_start", sort=True):
        out = process_batch(batch[EMPTY_WINDOWS[:7]], history, limits, settings.spc.baseline_windows,
                            detected_at=ws + pd.Timedelta(minutes=1))
        history = concat_nonempty([history, out.windows], EMPTY_WINDOWS)
        limits = concat_nonempty([limits, out.new_limits], LIMIT_COLS)
        new_limits.append(out.new_limits)
        scored.append(out.windows)
        alerts.append(out.alerts)
    alerts_df = concat_nonempty(alerts, list(alerts[0].columns))
    alerts_df["alert_id"] = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"{r.station_id}|{r.metric}|{r.rule_triggered}|{r.window_start}"))
        for r in alerts_df.itertuples()]
    return concat_nonempty(scored, EMPTY_WINDOWS), concat_nonempty(new_limits, LIMIT_COLS), alerts_df


def seed(settings: Settings, minutes: float = 40, start: datetime = START) -> dict:
    cfg = settings.generator
    messages = run_simulation(cfg, n_cells=int(minutes * 60 * cfg.rate), start=start)
    by_topic = {t: pd.DataFrame([m.value for m in messages if m.topic == t])
                for t in ("cell_tests", "assembly_events", "fault_log")}

    clean, quarantine = _silver_cell_tests(by_topic["cell_tests"])
    _write(settings, "silver", "cell_tests", clean, ["ts", "ingest_ts"])
    _write(settings, "silver", "cell_tests_quarantine", quarantine, ["ingest_ts", "quarantined_at"])

    assembly = _kafka_meta(by_topic["assembly_events"])
    assembly["ts"] = pd.to_datetime(assembly.ts, utc=True)
    assembly["ingest_ts"] = assembly.ts
    _write(settings, "silver", "assembly_events", assembly, ["ts", "ingest_ts"])
    changes = apply_assembly_events(pd.DataFrame(columns=GENEALOGY_COLS), {}, assembly)
    _write(settings, "silver", "cell_genealogy", changes.rows.assign(updated_at=assembly.ts.max()),
           ["valid_from", "valid_to", "updated_at"])
    _write(settings, "silver", "module_pack", changes.module_packs, ["packed_at"])

    faults = by_topic["fault_log"].assign(ingest_ts=lambda f: f.started_at)
    _write(settings, "silver", "fault_log", faults, ["started_at", "ingest_ts"])

    windows, limits, alerts = _spc(settings, clean)
    now = datetime.now(timezone.utc)
    _write(settings, "gold", "spc_window_stats", windows.assign(scored_at=now), ["window_start", "window_end", "scored_at"])
    _write(settings, "gold", "spc_control_limits", limits.assign(computed_at=now), ["computed_at"])
    _write(settings, "gold", "spc_alerts", alerts, ["window_start", "window_end", "detected_at"])
    return {
        "cell_test_messages": len(by_topic["cell_tests"]),
        "distinct_cell_tests": by_topic["cell_tests"].event_id.nunique(),
        "silver_cell_tests": len(clean),
        "quarantined": len(quarantine),
        "genealogy_rows": len(changes.rows),
        "spc_alerts": len(alerts),
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--minutes", type=float, default=40, help="minutes of simulated production")
    args = p.parse_args(argv)
    for k, v in seed(get_settings(), args.minutes).items():
        print(f"{k:<22} {v}")


if __name__ == "__main__":
    main()
