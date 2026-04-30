"""CheckpointStore: load semantics, atomic write, corruption handling."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from caseware_sync.core.errors import CheckpointCorruptError
from caseware_sync.domain.checkpoint import (
    EMPTY_UPDATED_AT,
    Checkpoint,
    TableWatermark,
)
from caseware_sync.state.checkpoint_store import CheckpointStore


@pytest.fixture
def store(tmp_path: Path) -> CheckpointStore:
    return CheckpointStore(tmp_path / "state" / "checkpoint.json", ["customers", "cases"])


class TestLoad:
    def test_missing_file_returns_initial(self, store: CheckpointStore) -> None:
        cp = store.load()
        assert cp.watermark("customers").updated_at == EMPTY_UPDATED_AT
        assert cp.watermark("cases").last_pk == 0
        # load() must NOT create the file.
        assert not store.path.exists()

    def test_partial_checkpoint_yields_empty_for_missing_table(
        self, store: CheckpointStore
    ) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text(json.dumps(
            {"customers": {"updated_at": "2026-03-31T12:00:00Z", "last_pk": 30}}
        ))
        cp = store.load()
        assert cp.watermark("customers").last_pk == 30
        assert cp.watermark("cases").last_pk == 0  # empty fallback

    @pytest.mark.parametrize("body", ["not-json{", '"hello"', "[]"])
    def test_corrupt_file_raises(self, store: CheckpointStore, body: str) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text(body)
        with pytest.raises(CheckpointCorruptError):
            store.load()

    def test_invalid_watermark_shape_raises(self, store: CheckpointStore) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text(json.dumps(
            {"customers": {"updated_at": "not-a-date", "last_pk": 1}}
        ))
        with pytest.raises(CheckpointCorruptError):
            store.load()


class TestCommit:
    def test_atomic_with_no_temp_leftovers(self, store: CheckpointStore) -> None:
        cp = Checkpoint.from_dict({
            "customers": {"updated_at": "2026-03-31T12:00:00Z", "last_pk": 30},
            "cases": {"updated_at": "2026-03-31T12:00:00Z", "last_pk": 200},
        })
        store.commit(cp)
        assert store.load() == cp
        leftovers = list(store.path.parent.glob(".checkpoint.json.*"))
        assert leftovers == []

    def test_pretty_form_human_readable(self, store: CheckpointStore) -> None:
        cp = Checkpoint(per_table={
            "customers": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 30),
            "cases": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 200),
        })
        store.commit(cp)
        body = store.path.read_text()
        assert '"cases"' in body and '"customers"' in body
        assert body.endswith("\n")


class TestStageThenCommit:
    def test_stage_does_not_touch_live_file(self, store: CheckpointStore, tmp_path: Path) -> None:
        cp = Checkpoint(per_table={
            "customers": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 30),
            "cases": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 200),
        })
        staged = tmp_path / "state" / "runs" / "abc" / "checkpoint.json"
        store.stage(cp, staged)
        assert staged.exists()
        # Live file must remain untouched (in this case still missing).
        assert not store.path.exists()

    def test_commit_from_staged_swings_atomically(
        self, store: CheckpointStore, tmp_path: Path
    ) -> None:
        cp = Checkpoint(per_table={
            "customers": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 30),
            "cases": TableWatermark(datetime(2026, 3, 31, 12, tzinfo=timezone.utc), 200),
        })
        staged = tmp_path / "state" / "runs" / "abc" / "checkpoint.json"
        store.stage(cp, staged)
        store.commit_from_staged(staged)
        assert store.load() == cp
        assert not staged.exists()  # staged file must move, not copy
