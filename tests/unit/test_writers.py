"""Output writers: lake partitioning + append, share zero-delta + determinism."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from caseware_sync.domain.tables import CASES, CUSTOMERS
from caseware_sync.outputs.event_writer import EventWriter
from caseware_sync.outputs.lake_writer import LakeWriter
from caseware_sync.outputs.share_writer import ShareWriter


def utc(y, m, d, h=0, mi=0, s=0):
    return datetime(y, m, d, h, mi, s, tzinfo=timezone.utc)


# ============================================================================
# Lake
# ============================================================================


class TestLakeWriter:
    def test_empty_rows_no_op(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        assert w.stage_partitions(CUSTOMERS, [], tmp_path / "stage") == []

    def test_single_partition_canonical_jsonl(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        rows = [
            {"customer_id": 31, "name": "C031", "email": "c31@x", "country": "US", "updated_at": utc(2026, 4, 15, 12)},
            {"customer_id": 32, "name": "C032", "email": "c32@x", "country": "CA", "updated_at": utc(2026, 4, 15, 12)},
        ]
        out = w.stage_partitions(CUSTOMERS, rows, tmp_path / "stage")
        assert len(out) == 1 and out[0].date_key == "2026-04-15"
        body = out[0].staged_path.read_bytes().decode()
        # Sorted keys; Z-suffix.
        assert body.splitlines()[0] == (
            '{"country":"US","customer_id":31,"email":"c31@x","name":"C031",'
            '"updated_at":"2026-04-15T12:00:00Z"}'
        )

    def test_byte_identical_across_runs(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        rows = [
            {"customer_id": 31, "name": "C031", "email": "c31@x", "country": "US", "updated_at": utc(2026, 4, 15, 12)},
        ]
        a = w.stage_partitions(CUSTOMERS, rows, tmp_path / "stage_a")
        b = w.stage_partitions(CUSTOMERS, rows, tmp_path / "stage_b")
        assert a[0].staged_path.read_bytes() == b[0].staged_path.read_bytes()

    def test_multi_date_partition_split(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        rows = [
            {"case_id": 1, "customer_id": 1, "title": "t", "description": "d", "status": "open", "updated_at": utc(2026, 4, 15, 12)},
            {"case_id": 2, "customer_id": 1, "title": "t", "description": "d", "status": "open", "updated_at": utc(2026, 4, 16, 0)},
        ]
        out = w.stage_partitions(CASES, rows, tmp_path / "stage")
        assert [s.date_key for s in out] == ["2026-04-15", "2026-04-16"]

    def test_stage_merged_preserves_existing_bytes_verbatim(self, tmp_path: Path) -> None:
        """The stage-merged-content trick: existing live bytes must be
        preserved exactly, with new rows appended after. This is what
        prevents lake corruption on re-runs."""
        w = LakeWriter(tmp_path / "lake")
        live = tmp_path / "lake" / "cases" / "date=2026-04-15" / "data.jsonl"
        live.parent.mkdir(parents=True, exist_ok=True)
        prior = b'{"old":"row"}\n'
        live.write_bytes(prior)

        rows = [{"case_id": 1000, "customer_id": 2, "title": "n", "description": "d", "status": "open", "updated_at": utc(2026, 4, 15, 13)}]
        out = w.stage_partitions(CASES, rows, tmp_path / "stage")
        merged = out[0].staged_path.read_bytes()
        assert merged.startswith(prior)
        assert merged.count(b"\n") == 2

    def test_naive_datetime_rejected(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        with pytest.raises(ValueError):
            w.stage_partitions(
                CUSTOMERS,
                [{"customer_id": 1, "name": "x", "email": "x", "country": "US", "updated_at": datetime(2026, 1, 1)}],
                tmp_path / "stage",
            )

    def test_predict_matches_stage_paths(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        rows = [
            {"case_id": 1, "customer_id": 1, "title": "t", "description": "d", "status": "open", "updated_at": utc(2026, 4, 15, 12)},
            {"case_id": 2, "customer_id": 1, "title": "t", "description": "d", "status": "open", "updated_at": utc(2026, 4, 16, 0)},
        ]
        predicted = w.predict_partitions(CASES, rows)
        actual = w.stage_partitions(CASES, rows, tmp_path / "stage")
        assert predicted == [a.live_path for a in actual]

    def test_utc_calendar_partitioning(self, tmp_path: Path) -> None:
        w = LakeWriter(tmp_path / "lake")
        rows = [{"customer_id": 1, "name": "x", "email": "x", "country": "US",
                 "updated_at": datetime(2026, 4, 16, 4, 30, tzinfo=timezone.utc)}]
        out = w.stage_partitions(CUSTOMERS, rows, tmp_path / "stage")
        assert out[0].date_key == "2026-04-16"


# ============================================================================
# Share
# ============================================================================


class TestShareWriter:
    def test_zero_delta_returns_none(self, tmp_path: Path) -> None:
        w = ShareWriter(tmp_path / "share")
        out = w.stage_share(
            table=CUSTOMERS, rows=[], run_id="r", schema_fingerprint="f",
            checkpoint_after={}, run_share_staging=tmp_path / "stage",
        )
        assert out is None

    def test_record_shape_and_nested_datetime(self, tmp_path: Path) -> None:
        w = ShareWriter(tmp_path / "share")
        rows = [
            {"customer_id": 31, "name": "C031", "email": "c@x", "country": "US",
             "updated_at": utc(2026, 4, 15, 12)},
        ]
        ck = {"customers": {"updated_at": "2026-04-15T12:00:00Z", "last_pk": 32}}
        out = w.stage_share(
            table=CUSTOMERS, rows=rows, run_id="r1", schema_fingerprint="abc",
            checkpoint_after=ck, run_share_staging=tmp_path / "stage",
        )
        rec = json.loads(out.staged_path.read_bytes().splitlines()[0])
        assert rec["table"] == "customers"
        assert rec["op"] == "upsert"
        assert rec["customer_id"] == 31
        assert rec["updated_at"] == "2026-04-15T12:00:00Z"
        # The nested record must use the *same* canonical Z-suffix datetime.
        assert rec["record"]["updated_at"] == "2026-04-15T12:00:00Z"
        assert rec["checkpoint_after"] == ck

    def test_byte_for_byte_determinism(self, tmp_path: Path) -> None:
        w = ShareWriter(tmp_path / "share")
        rows = [
            {"customer_id": 31, "name": "C031", "email": "c@x", "country": "US",
             "updated_at": utc(2026, 4, 15, 12)},
        ]
        ck = {"customers": {"updated_at": "2026-04-15T12:00:00Z", "last_pk": 32}}
        a = w.stage_share(table=CUSTOMERS, rows=rows, run_id="r", schema_fingerprint="f",
                          checkpoint_after=ck, run_share_staging=tmp_path / "stage_a")
        b = w.stage_share(table=CUSTOMERS, rows=rows, run_id="r", schema_fingerprint="f",
                          checkpoint_after=ck, run_share_staging=tmp_path / "stage_b")
        assert a.staged_path.read_bytes() == b.staged_path.read_bytes()

    def test_cases_use_case_id_top_level(self, tmp_path: Path) -> None:
        w = ShareWriter(tmp_path / "share")
        rows = [{"case_id": 201, "customer_id": 1, "title": "T", "description": "d", "status": "open",
                 "updated_at": utc(2026, 4, 15, 12)}]
        out = w.stage_share(
            table=CASES, rows=rows, run_id="r", schema_fingerprint="f",
            checkpoint_after={}, run_share_staging=tmp_path / "stage",
        )
        rec = json.loads(out.staged_path.read_bytes().splitlines()[0])
        assert rec["case_id"] == 201
        assert "customer_id" not in rec  # PK at top level only
        assert rec["record"]["case_id"] == 201
        assert rec["record"]["customer_id"] == 1


# ============================================================================
# Events
# ============================================================================


class _FakeEvent:
    """Duck type matching Event.model_dump(mode='json')."""

    def __init__(self, payload: dict) -> None:
        self._p = payload

    def model_dump(self, mode: str = "json") -> dict:
        return self._p


class TestEventWriter:
    def test_writes_one_jsonl_line_per_event(self, tmp_path: Path) -> None:
        w = EventWriter(tmp_path / "events")
        evs = [
            _FakeEvent({"table": "cases", "delta_row_count": 15}),
            _FakeEvent({"table": "customers", "delta_row_count": 2}),
        ]
        out = w.stage_events(events=evs, run_id="r1", run_events_staging=tmp_path / "stage")
        assert out.run_id == "r1" and out.event_count == 2
        body = out.staged_path.read_bytes().decode()
        assert [json.loads(l)["table"] for l in body.splitlines() if l] == ["cases", "customers"]
