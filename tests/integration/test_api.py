"""End-to-end: POST /ingest via FastAPI TestClient.

We bypass the lifespan dependency injection by overriding
``app.state.orchestrator`` with a tmp-dir-rooted instance. This lets us
exercise the route handler without spawning a separate process.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from caseware_sync.api.app import app


def test_post_ingest_real(orchestrator) -> None:
    app.state.orchestrator = orchestrator
    with TestClient(app) as client:
        r = client.post("/ingest", params={"dry_run": "false"})
    assert r.status_code == 200
    body = r.json()
    assert body["dry_run"] is False
    assert "run_id" in body and "started_at" in body and "finished_at" in body
    by_table = {t["table"]: t for t in body["tables"]}
    assert by_table["customers"]["delta_row_count"] == 30
    assert by_table["cases"]["delta_row_count"] == 200


def test_post_ingest_dry_run(orchestrator) -> None:
    app.state.orchestrator = orchestrator
    with TestClient(app) as client:
        r = client.post("/ingest", params={"dry_run": "true"})
    assert r.status_code == 200
    body = r.json()
    assert body["dry_run"] is True
    by_table = {t["table"]: t for t in body["tables"]}
    assert by_table["customers"]["delta_row_count"] == 30


def test_health() -> None:
    with TestClient(app) as client:
        r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
