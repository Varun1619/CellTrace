# CellTrace project rules for Claude

## What this is
A local-first streaming data pipeline that simulates battery cell end-of-line
testing, detects quality drift with SPC, and provides cell to module to pack traceability.

## Stack (do not swap without asking)
- Python 3.11, Kafka (Docker, KRaft mode, no Zookeeper)
- PySpark 3.5 + delta-spark (Structured Streaming)
- Delta Lake medallion: bronze / silver / gold under ./lakehouse
- dbt-duckdb for gold models (reads Delta via DuckDB delta extension)
- Airflow (Docker) for batch rollups
- Streamlit dashboard
- pytest for tests

## Conventions
- All config in .env + config/settings.py; no hardcoded paths or ports
- Every module gets a pytest test
- Small, focused commits; explain what changed in the summary
- Run tests before saying a step is done
- Keep a running DECISIONS.md log of design choices and why

## Working rules
These come from obra/superpowers and everything-claude-code.

- Test first. Write the failing test, run it, see it fail for the right reason, then write the code.
- No completion claims without fresh evidence. Run the command, read the output, then report.
- Debug by root cause. Reproduce, form one hypothesis, test it. Don't stack guesses.
- Keep domain logic in plain Python functions (distributions, SPC rules, SCD2 merge, recall).
  Spark and Kafka code is a thin shell around them, so most tests need no broker or JVM.
- Files stay under about 400 lines and functions under about 50.
- Conventional commits: feat, fix, refactor, docs, test, chore, perf, ci.
- No secrets in code. Local dev credentials live only in .env, which is gitignored.
- Prose (README, docs, comments, commits) avoids em dashes, emoji and title-case headings.

## Running things
- The host is Windows. Spark, Kafka, dbt, Airflow and the dashboard all run in Docker
  through `make` targets, so behavior matches CI on Linux.
- `make test` runs pytest inside the app container. Pure-Python tests also run locally
  with `pytest -m "not spark"`.
