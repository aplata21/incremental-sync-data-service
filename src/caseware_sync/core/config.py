"""Process-wide settings, hydrated from environment variables.

The defaults match the spec exactly:
    ./state, ./lake, ./share, ./events
so a fresh checkout running ``uvicorn`` from the repo root produces the
output paths the spec expects without any extra wiring.

Settings are immutable for the lifetime of a process; tests can construct
their own ``Settings(...)`` and inject it via ``api.deps`` to redirect output
roots to a tmp dir.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- source database ----------------------------------------------------
    database_url: PostgresDsn = Field(
        default="postgresql://interop:interop@localhost:5432/interop",  # type: ignore[arg-type]
        description="Postgres DSN for the source database (read-only access).",
    )

    # --- HTTP server --------------------------------------------------------
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # --- output roots (paths from the spec) ---------------------------------
    state_dir: Path = Path("./state")
    lake_dir: Path = Path("./lake")
    share_dir: Path = Path("./share")
    events_dir: Path = Path("./events")

    # --- run_id formatting --------------------------------------------------
    # SHA-256 produces a 64-char hex digest. A stable prefix is shorter,
    # easier to grep, and still effectively unique at this scale.
    # The spec explicitly allows "the hex digest, or a stable prefix of it".
    run_id_hex_prefix_len: int = Field(default=32, ge=8, le=64)

    # --- derived paths ------------------------------------------------------
    @property
    def checkpoint_path(self) -> Path:
        return self.state_dir / "checkpoint.json"

    @property
    def runs_dir(self) -> Path:
        """Per-run staging dir root used as the commit journal."""
        return self.state_dir / "runs"

    @property
    def ingest_lock_path(self) -> Path:
        """Single-flight lock so two concurrent /ingest calls cannot race."""
        return self.state_dir / ".ingest.lock"


def load_settings() -> Settings:
    """Module-level loader so the api layer can swap implementations in tests."""
    return Settings()
