"""Run dbt for the CellTrace project from anywhere: python -m scripts.run_dbt build

Creates the folder for the external (Parquet) gold models first, since DuckDB's COPY
does not create directories.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from config.settings import REPO_ROOT, get_settings

DBT_DIR = REPO_ROOT / "dbt"


def dbt_args(argv: list[str]) -> list[str]:
    return [*argv, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]


def external_root() -> Path:
    return get_settings().lakehouse_root / "gold" / "dbt"


def main(argv: list[str] | None = None) -> int:
    from dbt.cli.main import dbtRunner

    external_root().mkdir(parents=True, exist_ok=True)
    (DBT_DIR / "target").mkdir(exist_ok=True)
    os.environ.setdefault("DBT_DUCKDB_PATH", str(DBT_DIR / "target" / "celltrace.duckdb"))
    result = dbtRunner().invoke(dbt_args(argv if argv is not None else sys.argv[1:]))
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
