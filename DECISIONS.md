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
SPC limits come from the first 5 one-minute windows. The bad lot (lot 5 of 5,000 cells)
starts around 6.7 minutes and station drift around 10 minutes, so neither one contaminates
the limits it is meant to trip.

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

### Measured on the first full run (20 minutes, 50 cells/s)
- Station drift on L2-S3 (IR ramping 0.25 mOhm over 10 minutes): first alert WE1 after 195 s
  wall clock, 98 s of event time, when the drift was 0.041 mOhm (3.4% of nominal).
- Bad lot (capacity -0.12 Ah): first alert WE1 after 155 s wall clock, 58 s event time.
- Unexplained alerts: 6 over 252 scored windows (2.4% per window), all on voltage.

### Known tradeoff: a 5-window baseline is short
With about 250 cells per window, estimating the center from 5 windows leaves it uncertain
by about 0.45 sigma of the window mean. The side-of-center rules (WE3, WE4) are sensitive to
that, which is the likely reason the false alarm rate is above the roughly 1.4% per point
that the four rules give with exact limits. Textbook Phase I uses 20 to 25 subgroups. That
would mean a 20-minute warm-up before any fault, so the demo keeps 5 and
`SPC_BASELINE_WINDOWS` makes it a one-line change.
