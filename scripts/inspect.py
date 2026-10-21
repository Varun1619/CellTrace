"""Print row counts and quality signals for the lakehouse tables.

    python -m scripts.inspect
"""

from __future__ import annotations

from config.settings import get_settings
from scripts.lakehouse import query, table_exists

TABLES = [
    ("bronze", "cell_tests"), ("bronze", "assembly_events"), ("bronze", "fault_log"),
    ("silver", "cell_tests"), ("silver", "cell_tests_quarantine"), ("silver", "assembly_events"),
    ("silver", "fault_log"), ("silver", "cell_genealogy"),
    ("gold", "spc_alerts"),
]

CHECKS = {
    "bronze cell_tests: distinct event_ids": """
        SELECT count(*) AS rows, count(DISTINCT json_extract_string(payload, '$.event_id')) AS distinct_event_ids
        FROM {bronze.cell_tests}""",
    "silver cell_tests: duplicates left (must be 0)": """
        SELECT count(*) - count(DISTINCT event_id) AS duplicate_rows FROM {silver.cell_tests}""",
    "quarantine reasons": """
        SELECT quarantine_reason, count(*) AS n FROM {silver.cell_tests_quarantine}
        GROUP BY 1 ORDER BY n DESC""",
    "reconciliation: bronze distinct = silver + quarantine distinct": """
        SELECT
          (SELECT count(DISTINCT json_extract_string(payload, '$.event_id')) FROM {bronze.cell_tests}) AS bronze_distinct,
          (SELECT count(*) FROM {silver.cell_tests}) AS silver_rows,
          (SELECT count(DISTINCT event_id) FROM {silver.cell_tests_quarantine}) AS quarantined_distinct""",
    "fault log": "SELECT fault_type, target, metric, started_at FROM {silver.fault_log} ORDER BY started_at",
}


def main() -> None:
    settings = get_settings()
    print(f"lakehouse: {settings.lakehouse_root}\n")
    for layer, name in TABLES:
        n = query(f"SELECT count(*) AS n FROM {{{layer}.{name}}}", settings)["n"][0] if table_exists(settings, layer, name) else "missing"
        print(f"{layer + '.' + name:<36} {n}")
    for title, sql in CHECKS.items():
        print(f"\n== {title}")
        try:
            print(query(sql, settings).to_string(index=False))
        except Exception as exc:  # a table may not exist yet early in the build
            print(f"  skipped: {exc.__class__.__name__}: {str(exc).splitlines()[0]}")


if __name__ == "__main__":
    main()
