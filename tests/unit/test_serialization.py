"""Unit tests for the canonical JSON + filesystem primitives.

These are the byte-stable foundations every other module relies on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from caseware_sync.utils.fs import (
    atomic_rename,
    ensure_dir,
    fsync_dir,
    staged_writer,
    write_bytes_atomic,
)
from caseware_sync.utils.jsonio import (
    canonical_json_bytes,
    canonical_json_line,
    iso_utc_z,
    parse_iso_utc,
    pretty_json_bytes,
)


class TestIsoUtc:
    def test_whole_seconds_omit_microseconds(self) -> None:
        dt = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
        assert iso_utc_z(dt) == "2026-03-31T12:00:00Z"

    def test_microseconds_preserved(self) -> None:
        dt = datetime(2026, 3, 31, 12, 0, 0, 123456, tzinfo=timezone.utc)
        assert iso_utc_z(dt) == "2026-03-31T12:00:00.123456Z"

    def test_round_trip(self) -> None:
        dt = datetime(2026, 3, 31, 12, 0, 0, 123456, tzinfo=timezone.utc)
        assert parse_iso_utc(iso_utc_z(dt)) == dt

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError):
            iso_utc_z(datetime(2026, 1, 1))

    def test_non_utc_timezone_normalized(self) -> None:
        from datetime import timedelta
        tz = timezone(timedelta(hours=5))
        dt_local = datetime(2026, 3, 31, 17, 0, 0, tzinfo=tz)  # 12:00 UTC
        assert iso_utc_z(dt_local) == "2026-03-31T12:00:00Z"


class TestCanonicalJson:
    def test_keys_sorted(self) -> None:
        a = canonical_json_bytes({"b": 1, "a": 2})
        b = canonical_json_bytes({"a": 2, "b": 1})
        assert a == b == b'{"a":2,"b":1}'

    def test_no_whitespace(self) -> None:
        assert canonical_json_bytes({"a": [1, 2]}) == b'{"a":[1,2]}'

    def test_datetime_uses_z_suffix(self) -> None:
        out = canonical_json_bytes({"t": datetime(2026, 3, 31, 12, tzinfo=timezone.utc)})
        assert out == b'{"t":"2026-03-31T12:00:00Z"}'

    def test_nested_determinism(self) -> None:
        a = canonical_json_bytes({"x": {"b": 1, "a": 2}, "y": [3, 1]})
        b = canonical_json_bytes({"y": [3, 1], "x": {"a": 2, "b": 1}})
        assert a == b

    def test_jsonl_line_has_trailing_newline(self) -> None:
        assert canonical_json_line({"a": 1}) == b'{"a":1}\n'

    def test_pretty_form_human_readable(self) -> None:
        body = pretty_json_bytes({"a": 1, "b": 2})
        assert body.startswith(b"{\n")
        assert b'"a": 1' in body
        assert body.endswith(b"\n")


class TestFs:
    def test_ensure_dir_idempotent(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "c"
        ensure_dir(target)
        ensure_dir(target)
        assert target.is_dir()

    def test_write_bytes_atomic_no_temp_leftovers(self, tmp_path: Path) -> None:
        target = tmp_path / "deep" / "out.txt"
        write_bytes_atomic(target, b"hello")
        assert target.read_bytes() == b"hello"
        leftovers = list(target.parent.glob(".out.txt.*"))
        assert leftovers == []

    def test_write_bytes_atomic_overwrites(self, tmp_path: Path) -> None:
        target = tmp_path / "out.txt"
        write_bytes_atomic(target, b"v1")
        write_bytes_atomic(target, b"v2")
        assert target.read_bytes() == b"v2"

    def test_atomic_rename(self, tmp_path: Path) -> None:
        src = tmp_path / "src.txt"
        src.write_text("hi")
        dst = tmp_path / "deep" / "dst.txt"
        atomic_rename(src, dst)
        assert dst.read_text() == "hi"
        assert not src.exists()

    def test_staged_writer_aborts_cleanly_on_exception(self, tmp_path: Path) -> None:
        target = tmp_path / "fail.txt"
        with pytest.raises(RuntimeError):
            with staged_writer(target) as tmp:
                tmp.write_bytes(b"partial")
                raise RuntimeError("boom")
        assert not target.exists()
        assert list(tmp_path.glob(".fail.txt.*")) == []

    def test_staged_writer_commits_on_success(self, tmp_path: Path) -> None:
        target = tmp_path / "ok.txt"
        with staged_writer(target) as tmp:
            tmp.write_bytes(b"committed")
        assert target.read_bytes() == b"committed"

    def test_fsync_dir_tolerant_of_missing(self, tmp_path: Path) -> None:
        # Must not raise on a non-existent dir.
        fsync_dir(tmp_path / "nope")
