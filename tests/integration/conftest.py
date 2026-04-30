"""Integration-test fixtures.

Requires the dockerized Postgres from ``docker-compose.yml`` to be running.
Each test starts from a freshly-seeded DB (drop + re-apply ``db/init.sql``)
and a clean per-test tmp output dir.

Run with:
    DATABASE_URL=postgresql://interop:interop@localhost:5432/interop \
        pytest tests/integration -m integration
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import psycopg
import pytest

from caseware_sync.api.deps import build_orchestrator
from caseware_sync.core.config import Settings

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DB_DIR = REPO_ROOT / "db"


# --------------------------------------------------------------- session DSN


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    dsn = os.environ.get(
        "DATABASE_URL", "postgresql://interop:interop@localhost:5432/interop"
    )
    deadline = time.time() + 30
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=2) as c:
                c.execute("SELECT 1")
            return dsn
        except Exception as e:  # pragma: no cover - timing-dependent
            last_err = e
            time.sleep(1)
    pytest.skip(f"Postgres not reachable at {dsn}: {last_err}")


# ------------------------------------------------------------ per-test reset


@pytest.fixture
def fresh_db(pg_dsn: str) -> str:
    """Drop and recreate the schema before each test."""
    init_sql = (DB_DIR / "init.sql").read_text()
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS cases CASCADE")
            cur.execute("DROP TABLE IF EXISTS customers CASCADE")
        with conn.cursor() as cur:
            cur.execute(init_sql)
    return pg_dsn


@pytest.fixture
def apply_changes_sql(pg_dsn: str):
    """Helper that callers use after the first ingest to simulate updates."""
    def _apply():
        sql = (DB_DIR / "changes.sql").read_text()
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    return _apply


# ---------------------------------------------------------- settings + orch


@pytest.fixture
def settings(tmp_path: Path, fresh_db: str) -> Settings:
    """Settings rooted in a tmp dir, pointed at the dockerized DB."""
    return Settings(
        database_url=fresh_db,  # type: ignore[arg-type]
        state_dir=tmp_path / "state",
        lake_dir=tmp_path / "lake",
        share_dir=tmp_path / "share",
        events_dir=tmp_path / "events",
    )


@pytest.fixture
def orchestrator(settings: Settings):
    return build_orchestrator(settings)


# ----------------------------------------------------------- common helpers


@pytest.fixture
def db_count(pg_dsn: str):
    def _count(table: str) -> int:
        with psycopg.connect(pg_dsn) as c, c.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table}")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    return _count


# Auto-apply 'integration' marker so tests in this folder can be selected
# with `-m integration` / skipped without docker.


def pytest_collection_modifyitems(config, items) -> None:
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
