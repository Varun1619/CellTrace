"""Genealogy stream over a temp lakehouse: MERGE semantics across micro-batches."""

from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.spark

T0 = datetime(2026, 1, 5, 8, 0)


def assembly_row(offset, kind, cell=None, module=None, pack=None, prev=None):
    ts = T0 + timedelta(seconds=offset)
    return (f"a{offset}", kind, "L1", cell, module, pack, prev, ts, 0, offset, ts)


def test_genealogy_stream_versions_reworked_cells_across_batches(spark, lake):
    from streaming.genealogy import start_genealogy
    from streaming.tables import SILVER_ASSEMBLY, SILVER_TABLES, ensure_tables

    ensure_tables(spark, lake, SILVER_TABLES.values())
    path = lake.table_path("silver", "assembly_events")
    batches = [
        [assembly_row(1, "CELL_TO_MODULE", "C1", "M1"), assembly_row(2, "CELL_TO_MODULE", "C2", "M1"),
         assembly_row(3, "MODULE_TO_PACK", module="M1", pack="P1")],
        [assembly_row(4, "CELL_TO_MODULE", "C3", "M2"), assembly_row(5, "CELL_REWORK", "C1", "M2", prev="M1")],
        [assembly_row(6, "MODULE_TO_PACK", module="M2", pack="P2")],
    ]
    for rows in batches:
        spark.createDataFrame(rows, SILVER_ASSEMBLY).coalesce(1).write.format("delta").mode("append").save(path)

    for q in start_genealogy(spark, lake, trigger={"availableNow": True}, source_options={"maxFilesPerTrigger": 1}):
        q.awaitTermination(180)

    g = spark.read.format("delta").load(lake.table_path("silver", "cell_genealogy")).toPandas()
    current = {r.cell_id: (r.module_id, r.pack_id) for r in g[g.is_current].itertuples()}
    assert current == {"C1": ("M2", "P2"), "C2": ("M1", "P1"), "C3": ("M2", "P2")}
    c1 = g[g.cell_id == "C1"].sort_values("valid_from")
    assert list(c1.module_id) == ["M1", "M2"]
    assert list(c1.is_current) == [False, True]
    assert c1.iloc[0].valid_to == T0 + timedelta(seconds=5)
    assert g.groupby("cell_id").is_current.sum().eq(1).all()

    packs = spark.read.format("delta").load(lake.table_path("silver", "module_pack")).toPandas()
    assert dict(zip(packs.module_id, packs.pack_id)) == {"M1": "P1", "M2": "P2"}
