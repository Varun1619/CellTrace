# Design decisions

A running log of choices and the reasons behind them. Newest entries go at the bottom of each phase.

## Phase 1: repo layout and runtime

### Everything runs in Docker, including tests
The dev machine is Windows. Spark writing Delta tables to a local Windows filesystem needs
`winutils.exe` and `hadoop.dll`, which is fragile setup that no reviewer will reproduce.
One `celltrace-app` image (Python 3.11, Java 17, PySpark 3.5, delta-spark, dbt-duckdb,
Streamlit) runs the generator, streaming jobs, dbt, scripts, tests and dashboard. The repo is
bind-mounted at `/app`, so code edits never need an image rebuild. CI runs the same pytest
suite natively on Ubuntu.

### Jars resolved at build time
`configure_spark_with_delta_pip` downloads the Delta and Kafka connector jars from Maven on
first use. The Dockerfile builds one throwaway SparkSession so the jars sit in the image's Ivy
cache and containers start without network access to Maven.

### Kafka in KRaft mode with two listeners
`kafka:29092` is the in-network listener for containers and `localhost:9092` is for host
tools. Auto topic creation is off. A one-shot `kafka-init` service creates the topics
explicitly so partition counts are intentional:
- `cell_tests`, 3 partitions, keyed by `station_id` so each station's readings stay ordered.
- `assembly_events`, 3 partitions, keyed by `line_id` so a line's cell, module and pack
  events stay ordered.
- `fault_log`, 1 partition. It is tiny and order matters.

### Portable Makefile
GNU make on Windows falls back to `cmd.exe` when no `sh` is on the PATH. Recipes use only
`docker compose` and `python -c`, which behave the same under both shells.

### Lakehouse on a named volume by default
Measured inside the streaming container: 500 small-file write+rename cycles took 56 s on
the Windows bind mount and 10.8 s on container storage. Delta logs and streaming checkpoints
are exactly that workload, and micro-batches with a 10 s trigger were taking 20 to 50 s.
`LAKEHOUSE_MOUNT` now defaults to a named volume. Setting it to `./lakehouse` brings back
host-visible files at the cost of speed.

## Phase 2: generator

### Simulation core is pure and seeded
`generator/simulator.py` takes timestamps and returns messages. It never reads the clock
or touches Kafka. Tests drive it with a simulated clock and check distributions, fault
shapes and assembly invariants in about 15 seconds without a broker. The same seed gives
byte-identical output.

### One spec table for PASS/FAIL
Spec limits sit in `dbt/seeds/spec_limits.csv`. The generator decides PASS/FAIL from it
and the dbt models derive failure causes from it, so the two cannot drift apart.
Plausibility ranges (used for quarantine) are separate and much wider. A value outside
them means a broken sensor, not a bad cell.

### Faults start after the SPC baseline
SPC limits come from the first 20 one-minute windows. The bad lot (lot 15 of 5,000 cells)
starts around minute 23 and station drift at minute 26, so neither one contaminates the
limits it is meant to trip. (The first full run used a 5-window baseline with faults at
minutes 7 and 10; see Phase 4 for why that changed.)

### IDs carry a run tag
Cell, lot, module and pack IDs include the generator start time (`C-10211557-00000001`).
Restarting the generator cannot collide with an earlier run's lot IDs, which would
otherwise corrupt recall lookups.

### Late and duplicate events are held, not backdated
A late event keeps its original `ts` and is released 10 to 300 s later. A duplicate is the
identical payload re-sent 0.5 to 30 s later. That is how real out-of-order and at-least-once
delivery looks to the consumer.

## Phase 3: bronze and silver

### Bronze stores raw strings
Bronze keeps the Kafka key, the payload as text, and topic, partition, offset and ingest
time. Nothing is parsed, so a schema bug in silver can be fixed and replayed from bronze.

### Tables are created before streams start
`streaming/tables.py` holds every schema and creates empty Delta tables up front. A
downstream stream never fails because its upstream has not committed yet, and schemas are
explicit, not inferred.

### Quarantine keeps every reason
A row with two problems gets `null_capacity_ah;out_of_range_temp_c`, not just the first
reason. The raw payload is kept so the row can be reprocessed.

### Dedup uses dropDuplicatesWithinWatermark
Spark 3.5's `dropDuplicatesWithinWatermark` keeps the first copy of each `event_id`
and expires state 10 minutes past the event time. Plain `dropDuplicates` only bounds its
state if the event-time column is part of the key, so two copies of one event with
different timestamps (a producer that re-stamps on retry) would both survive.

### Quality checks run inside the silver sink
The cell-test and assembly sinks are `foreachBatch` writers. Each batch is checked
(not-null keys, accepted values, plausible ranges, unique `event_id`) in one aggregation,
metrics go to `silver.dq_batch_metrics`, and a failing batch stops the stream instead of
writing bad data. Appends use Delta's `txnAppId`/`txnVersion`, so a replayed batch after
a crash is not written twice.

### Verified on a 5-minute run
14,999 generated cells, 15,170 bronze rows (1.1% duplicates), 14,923 silver rows, 76
quarantined, 0 duplicates left in silver. Silver plus quarantine reconciles exactly to the
generator's count.

## Phase 4: SPC drift detection

### X-bar chart on 1-minute subgroups with variable n
Each station and metric is a series of 1-minute windows. Limits come from the first 5
windows: the center is the n-weighted grand mean and sigma is the pooled within-window
standard deviation (the X-bar/S approach). A later window is scored as
`z = (mean - center) / (sigma / sqrt(n))`, so a window with fewer cells gets proportionally
wider limits instead of false alarms.

### Rules live in plain Python
`streaming/spc_rules.py` holds the limits, scoring and the four Western Electric rules, with
no Spark import. Each rule has a test with a known z sequence, and drift tests check that
the right rule fires on the right window. `spc_monitor.py` is a thin Spark shell: a
watermarked tumbling-window aggregation whose `foreachBatch` hands closed windows to the
rules and appends what comes back.

### History is read back from the gold table
WE2 to WE4 need the previous points of a series. Each batch reads the last two hours of
`gold.spc_window_stats` plus any unscored baseline rows. That keeps the streaming state
small and makes the history visible to the dashboard for free.

### Alerts are idempotent
`alert_id` is a UUID5 of station, metric, rule and window start, and every append uses
`txnAppId`/`txnVersion`. Replaying a batch after a crash cannot duplicate alerts.

### Two clocks for time-to-detect
`scripts/detection.py` reports fault start to the end of the first alerting window (event
time, the statistical delay) and fault start to when the alert was written (wall clock,
which adds the window, the 1-minute watermark and three 10-second trigger hops). On the
first full run the statistics needed one window and the pipeline added about 97 seconds.

### Measured on the first full run (20 minutes, 50 cells/s, 5-window baseline)
- Station drift on L2-S3 (IR ramping 0.25 mOhm over 10 minutes): first alert WE1 after 195 s
  wall clock, 98 s of event time, when the drift was 0.041 mOhm (3.4% of nominal).
- Bad lot (capacity -0.12 Ah): first alert WE1 after 155 s wall clock, 58 s event time.
- Unexplained alerts: 6 over 252 scored windows (2.4% per window), all on voltage.

### The baseline went from 5 windows to 20
With about 250 cells per window, estimating the center from 5 windows leaves it uncertain
by about 0.45 sigma of the window mean, and the side-of-center rules (WE3, WE4) fire on series
that are in control. On the first run 6.25% of scored windows raised an alert no fault
explained (18 episodes on 13 of 36 series).

Textbook Phase I uses 20 to 25 subgroups, so the default is now 20, with the bad lot moved
to minute 23 and the drift to minute 26. On a 38-minute run the same measure fell to 3.10%
(612 scored windows), with no loss of speed: drift was caught in 191 s at 0.036 mOhm, against
195 s at 0.041 mOhm before, because a tighter center line also shrinks the miss. The cost is
a 20-minute warm-up before monitoring starts. The four rules alone would give about 1.1% per
window with exact limits (an in-control average run length of about 92).

### Attribution follows each rule's lookback
WE2, WE3 and WE4 look back 2, 4 and 7 windows. A WE4 alert minutes after a bad lot can be
caused by the lot's windows inside its lookback. `scripts/detection.py` now counts an alert as
explained when the earliest window its rule looked at overlaps the fault (rule lookbacks
come from `RULE_LOOKBACK` in `spc_rules.py`). The bad lot's grace period also dropped from
two windows to one, because SPC discards events later than its 1-minute watermark. With
this, the 38-minute run's unexplained rate is 2.45%: 15 alerts in 11 episodes on 9 series.

## Phase 5: genealogy and recall

### SCD Type 2 keyed on (cell_id, valid_from)
A rework closes the current version (`valid_to` = rework time, `is_current` false) and opens
a new one. Each cell always has exactly one current row, and its full module history
survives, so a recall can say "this cell was in M1, then reworked into M2".

### Pack assignment is a Type 1 fill, not a new version
Modules get their pack after their cells are placed. When `MODULE_TO_PACK` arrives, the open
versions of that module's cells get `pack_id` filled in place. The cell did not move; the
pack just became known. Making it a new version would double the table and make every cell
look reworked.

### Events apply in Kafka order
The generator keys assembly events by line, so a line's events share one partition and
`(partition, offset)` is their true order. Event timestamps can tie at millisecond
resolution, which would make "rework then place" ambiguous.

### Pure rules, one MERGE per batch
`streaming/genealogy_rules.py` applies a batch of events to the current rows they touch and
returns every changed version. `genealogy.py` reads only the current rows for the batch's
cells and modules, then does one Delta MERGE on the SCD2 key. Replaying a batch is a no-op
because a placement into the module a cell is already in is skipped.

### Recall runs on DuckDB, not Spark
`scripts/recall_lookup.py` is one SQL query over `silver.cell_tests` and
`silver.cell_genealogy` through the DuckDB delta extension. No JVM start, so a lookup takes
milliseconds. It reports cells that failed at test (never assembled) and cells that passed
but are not yet in a module, because a recall that only lists packs hides the cells still
on the floor.

## Phase 6: dbt

### Marts are Parquet files, not DuckDB tables
dbt-duckdb's external materialization writes each mart to `lakehouse/gold/dbt/*.parquet`.
The dashboard reads those files directly, so it never contends with `dbt build` for the
DuckDB file lock. Airflow also keeps its own DuckDB file for the same reason.

### Failure causes come from the shared spec seed
`stg_cell_failures` unpivots the readings and joins `spec_limits`, one row per violated
spec. The Pareto counts causes, not cells, and its docs say so.

### Lot outliers need statistical and practical significance
`fct_lot_quality` compares each lot to the median lot. z uses the lot's own standard error
and effect size is the shift in within-lot standard deviations. A lot is flagged only when
|z| > 4 and the effect is at least 0.5 SD. On the first run the bad lot scored z = -168 and
effect -2.4 SD. Lot 8 had an IR z of 4.4 because it was tested while L2-S3 was drifting, but
its effect was well under 0.5 SD, and correctly it was not flagged: that is a station
problem, and SPC already caught it.

### Singular tests encode the business rules
`assert_injected_bad_lot_is_flagged` and `assert_no_normal_lot_is_flagged` fail the build if
the outlier thresholds stop separating the injected lot from normal ones.

## Phase 7: Airflow

### Airflow 2.10, not 3.x
The DAG needs task SLAs and `sla_miss_callback`, which Airflow 3.0 removed.

### dbt and delta-rs in their own virtualenv
dbt-core pins protobuf, Jinja and others that collide with Airflow's constraints. The image
installs them into `/opt/airflow/celltrace-venv` and tasks call that interpreter through
`BashOperator`.

### Maintenance with delta-rs instead of Spark
OPTIMIZE and VACUUM run through the `deltalake` Python package, so the Airflow image needs no
Java. Compaction commits with `dataChange=false`, which the Spark streams reading silver
ignore, and a commit conflict with a streaming append is retried. VACUUM keeps 7 days and
refuses less.

### One UID for every writer
The app and Airflow images both run as UID 50000, so Spark, dbt and the maintenance tasks can
all write the shared lakehouse volume.

### Daily report upserts by date
`publish_daily_report` MERGEs on `test_date`, so a retry or a backfill replaces a day's row.

### Verified on the first full run
Both DAG runs (the manual trigger and the scheduled run created on unpause) finished with all
three tasks green in about 30 seconds. OPTIMIZE compacted `silver.cell_tests` from 408 files
to 1 and `silver.assembly_events` from 416 to 1. VACUUM removed nothing, as expected with
7-day retention. The bad-lot recall went from 670 ms before compaction to 340 ms after (one
measurement each), because DuckDB no longer opens hundreds of small Parquet files.

### DAG tests use a throwaway SQLite database
`DagBag` needs a metadata database. `make test-airflow` and CI migrate a temporary SQLite
database instead of depending on the Postgres service.

## Phase 8: dashboard

### Queries, charts and layout are separate
`queries.py` returns DataFrames, `charts.py` turns DataFrames into Plotly figures, and
`app.py` only lays them out. Both of the first two are tested without a browser.

### The Live SPC tab is a fragment
`st.fragment(run_every=10)` reruns only the control chart and its alert table, not the whole
page. Queries are cached for 10 seconds so several viewers do not multiply the load.

### Chart rules
Colors come from a validated dark palette (three categorical slots that pass the all-pairs
colorblind checks). Control limits are muted reference lines, alerts use a reserved red with
an X marker and the rule name, and the Pareto is a single-axis ranked bar chart with the
cumulative share in the hover instead of the classic dual-axis Pareto.

## Phase 9: CI and seeding

### A seeder built from the pure cores
`scripts/seed_lakehouse.py` runs the simulator, the silver rules, the genealogy rules and
the SPC engine in-process and writes every table with delta-rs. CI uses it to give
`dbt build` real data without Kafka or Spark. Seeded alerts have no pipeline latency.

### Measured throughput and where the limit is
A 2-minute burst at 1,000 cells/s (1,768 messages/s with assembly events) reached silver in
full 189 s after the producer stopped: about 736 messages/s, or 400 cells/s, sustained end to
end, 8x the default rate. `BatchTimer` logs each sink's stages per micro-batch. They show a
fixed cost of 20 to 40 s per batch whether it holds 500 or 35,000 rows: state-store and
Delta commits, with 13 streaming queries sharing 8 cores. The pandas and rule work is under
a second. Two consequences:
- Longer triggers raise throughput at the cost of detection latency.
- Genealogy's `read_current` scans the whole genealogy table each batch (20 to 43 s by the
  end of the burst) and grows with it. At scale the current versions belong in their own
  small table or a table clustered by `cell_id`.

### Surviving a sleeping laptop
The dev machine suspended several times during long runs. Spark local mode declared its only
executor lost after the 120 s default `spark.network.timeout` and stopped the context. The
timeout is now an hour, and the streaming service has `restart: unless-stopped`. A restart
is safe because every source resumes from its checkpoint and every sink is idempotent.
After one such crash, an anti-join showed every bronze event in silver or quarantine exactly
once.

### Counting false alarms as episodes
WE3 and WE4 keep firing on every window while a series stays off-center, so one condition
can produce six alerts. The README reports unexplained alerts as episodes (consecutive
alerting windows on one series) alongside the raw per-window count.
