"""Shared fixtures for the whole suite.

Unit tests get tmp dirs and small builders; integration tests extend this
with a Postgres-backed ``fresh_db`` fixture.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def out_dirs(tmp_path: Path) -> SimpleNamespace:
    """The four spec output roots, rooted under the test's tmp dir."""
    return SimpleNamespace(
        state=tmp_path / "state",
        lake=tmp_path / "lake",
        share=tmp_path / "share",
        events=tmp_path / "events",
    )
