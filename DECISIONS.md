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

### Lakehouse location is configurable
The lakehouse defaults to a bind mount at `./lakehouse` so the Delta files are visible from
the host. `LAKEHOUSE_MOUNT` can point it at a named Docker volume if bind-mount I/O on
Windows turns out to be the bottleneck.

### Portable Makefile
GNU make on Windows falls back to `cmd.exe` when no `sh` is on the PATH. Recipes use only
`docker compose` and `python -c`, which behave the same under both shells.
