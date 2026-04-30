"""FastAPI app for the incremental-sync prototype.

Exposes exactly one route the spec asks for:
    POST /ingest?dry_run=true|false

Plus a tiny ``/health`` for ops sanity. Errors are mapped to HTTP codes
in a single place so the route handler reads top-to-bottom without
exception sprawl.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from ..core.errors import (
    CheckpointCorruptError,
    CommitInterruptedError,
    IngestInProgressError,
    SourceQueryError,
)
from ..core.logging import configure_logging, get_logger
from .deps import init_orchestrator

log = get_logger("caseware_sync.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    orch = init_orchestrator()
    # Bring ./state/runs/ to a clean state at boot. Any leftover crashed
    # runs are completed (if their READY marker was written) or discarded
    # (if not). After this returns, the next /ingest call starts from a
    # consistent live filesystem.
    try:
        resumed = orch.startup_resume()
        if resumed:
            log.info("startup: resumed/cleaned %d run dir(s): %s", len(resumed), resumed)
    except Exception:
        # Startup-time resume failures should not silently leave the
        # service "up but broken". Re-raise to abort startup.
        log.exception("startup_resume failed")
        raise
    app.state.orchestrator = orch
    yield


app = FastAPI(
    title="Caseware Incremental Sync",
    description="Incremental sync + data sharing prototype",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest")
def ingest(dry_run: bool = False) -> JSONResponse:
    """Run one incremental ingest and return the manifest.

    Query params:
        dry_run (bool, default false):
            true  -> compute deltas + manifest, write nothing, do not
                     advance the on-disk checkpoint.
            false -> stage outputs, run the crash-consistent commit,
                     advance the checkpoint, return the manifest.

    Errors:
        409 if another /ingest is in progress.
        500 if the checkpoint file is corrupt.
        502 if the source DB query fails.
        500 if a stage/commit fails partway (after which a retry will
            resume the partial state automatically).
    """
    orch = app.state.orchestrator
    try:
        manifest = orch.run(dry_run=dry_run)
    except IngestInProgressError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except CheckpointCorruptError as e:
        raise HTTPException(status_code=500, detail=f"checkpoint corrupt: {e}") from e
    except CommitInterruptedError as e:
        raise HTTPException(status_code=500, detail=f"commit interrupted: {e}") from e
    except SourceQueryError as e:
        raise HTTPException(status_code=502, detail=f"source error: {e}") from e
    return JSONResponse(content=manifest.model_dump(mode="json"))
