"""Read Delta tables from DuckDB without a Spark session (scripts, dbt helpers, dashboard)."""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

from config.settings import Settings, get_settings


def connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with the delta extension (preinstalled in the app image, downloaded elsewhere)."""
    con = duckdb.connect()
    try:
        con.load_extension("delta")
    except duckdb.Error:
        con.install_extension("delta")
        con.load_extension("delta")
    return con


def table_exists(settings: Settings, layer: str, name: str) -> bool:
    return (Path(settings.table_path(layer, name)) / "_delta_log").is_dir()


def scan(settings: Settings, layer: str, name: str) -> str:
    """SQL fragment for a Delta table, for use in FROM clauses."""
    path = settings.table_path(layer, name).replace("'", "''")
    return f"delta_scan('{path}')"


def query(sql: str, settings: Settings | None = None, params: list | None = None):
    """Run SQL with {layer.name} placeholders expanded to delta_scan() calls; returns a pandas frame."""
    s = settings or get_settings()
    con = connect()
    try:
        return con.execute(expand(sql, s), params or []).df()
    finally:
        con.close()


def expand(sql: str, settings: Settings) -> str:
    return re.sub(r"\{(bronze|silver|gold)\.(\w+)\}", lambda m: scan(settings, m.group(1), m.group(2)), sql)
