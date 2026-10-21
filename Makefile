# Recipes avoid shell-specific syntax so they run under sh (Linux, macOS, Git Bash)
# and under cmd.exe (GNU make on Windows without sh). Pass extra flags with ARGS="...".

COMPOSE = docker compose
RUN = $(COMPOSE) run --rm
PY = python
ARGS ?=

.PHONY: env build up down generate stream pipeline dbt dashboard test test-local inspect recall logs

env:
	@$(PY) -c "import os, shutil; os.path.exists('.env') or shutil.copy('.env.example', '.env')"

build: env
	$(COMPOSE) build app

up: env
	$(COMPOSE) up -d --wait kafka kafka-ui
	$(RUN) kafka-init

down:
	$(COMPOSE) --profile "*" down

generate: env
	$(RUN) generator python -m generator.cell_test_producer $(ARGS)

stream: env
	$(RUN) --service-ports streaming python -m streaming.run_all $(ARGS)

pipeline: up
	$(COMPOSE) up -d generator streaming

dbt: env
	$(RUN) app sh -c "cd dbt && dbt build --profiles-dir . && dbt docs generate --profiles-dir ."

dashboard: env
	$(COMPOSE) up -d dashboard
	@echo Dashboard at http://localhost:8501

test: env
	$(RUN) app pytest -q $(ARGS)

test-local:
	$(PY) -m pytest -q -m "not spark" $(ARGS)

inspect: env
	$(RUN) app python -m scripts.inspect $(ARGS)

recall: env
	$(RUN) app python -m scripts.recall_lookup $(ARGS)

logs:
	$(COMPOSE) logs -f --tail 100 $(ARGS)
