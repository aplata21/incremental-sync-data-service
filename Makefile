# =============================================================================
# Caseware incremental-sync prototype
#
# Common workflow:
#   make up        # start Postgres (auto-applies db/init.sql on empty volume)
#   make install   # editable install of the service + dev deps
#   make run       # start FastAPI on PORT (default 8000)
#   make ingest    # POST /ingest?dry_run=false
#   make changes   # apply db/changes.sql between two ingests
#   make ingest    # POST /ingest again to validate incremental delta
#   make test      # unit + integration tests (integration uses the running db)
#   make reset     # wipe local outputs (state/, lake/, share/, events/)
#   make down      # stop and *remove* the postgres volume (full reset)
# =============================================================================

PY    ?= python
PORT  ?= 8000
HOST  ?= 0.0.0.0
DB_URL ?= postgresql://interop:interop@localhost:5432/interop

.PHONY: help up down wait seed-info changes reset install run test ingest dry-ingest \
        unit integration lint typecheck

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## start dockerized postgres (idempotent)
	docker compose up -d --wait

down: ## stop postgres and drop the data volume
	docker compose down -v

wait: ## wait until postgres is healthy
	@until docker compose exec -T postgres pg_isready -U interop -d interop >/dev/null 2>&1; do \
	  sleep 1; \
	done
	@echo "postgres ready"

seed-info: ## show seed counts (sanity check)
	@docker compose exec -T postgres psql -U interop -d interop -c \
	  "SELECT (SELECT count(*) FROM customers) AS customers, (SELECT count(*) FROM cases) AS cases;"

changes: ## apply db/changes.sql between ingests
	docker compose exec -T postgres psql -U interop -d interop < db/changes.sql

reset: ## wipe local outputs (NOT the database)
	rm -rf state lake share events
	@echo "local outputs cleaned"

install: ## install service + dev deps (editable)
	$(PY) -m pip install -e ".[dev]"

run: ## start FastAPI service
	DATABASE_URL=$(DB_URL) uvicorn caseware_sync.api.app:app --host $(HOST) --port $(PORT) --reload

ingest: ## POST /ingest?dry_run=false
	curl -s -X POST "http://localhost:$(PORT)/ingest?dry_run=false" | $(PY) -m json.tool

dry-ingest: ## POST /ingest?dry_run=true
	curl -s -X POST "http://localhost:$(PORT)/ingest?dry_run=true" | $(PY) -m json.tool

test: ## run all tests
	pytest

unit: ## run unit tests only
	pytest tests/unit

integration: ## run integration tests (requires `make up`)
	DATABASE_URL=$(DB_URL) pytest tests/integration -m integration

lint:
	ruff check src tests

typecheck:
	mypy src
