"""Unit tests for the pure pipeline helpers: run_id, fingerprint, advance.

These functions are the heart of "same source state + same checkpoint
produce the same outputs". If any of them lose determinism, replay
safety is gone.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from caseware_sync.domain.checkpoint import (
    EMPTY_UPDATED_AT,
    Checkpoint,
    TableWatermark,
)
from caseware_sync.domain.tables import CASES, CUSTOMERS, Column, TableSpec
from caseware_sync.pipeline.checkpoint_advance import (
    advance_checkpoint,
    build_row_identities,
)
from caseware_sync.pipeline.fingerprint import compute_schema_fingerprint
from caseware_sync.pipeline.run_id import RowIdentity, compute_run_id


def utc(y, m, d, h=0, mi=0, s=0):
    return datetime(y, m, d, h, mi, s, tzinfo=timezone.utc)


SPECS = {"customers": CUSTOMERS, "cases": CASES}


# ---------------------------------------------------------------- run_id


class TestRunId:
    def setup_method(self) -> None:
        self.ckpt = {
            "customers": {"updated_at": "2026-03-31T12:00:00Z", "last_pk": 30},
            "cases": {"updated_at": "2026-03-31T12:00:00Z", "last_pk": 200},
        }
        self.deltas = {
            "customers": [
                RowIdentity("customers", 31, utc(2026, 4, 15, 12)),
                RowIdentity("customers", 32, utc(2026, 4, 15, 12)),
            ],
            "cases": [
                RowIdentity("cases", 1, utc(2026, 4, 15, 12)),
                RowIdentity("cases", 50, utc(2026, 4, 15, 12)),
            ],
        }

    def test_deterministic_across_calls(self) -> None:
        r1 = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        r2 = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        assert r1 == r2 and len(r1) == 32

    def test_independent_of_table_key_order(self) -> None:
        flipped = {"cases": self.deltas["cases"], "customers": self.deltas["customers"]}
        a = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        b = compute_run_id(checkpoint_before=self.ckpt, table_deltas=flipped)
        assert a == b

    def test_changes_with_checkpoint(self) -> None:
        other = dict(self.ckpt)
        other["customers"] = {**other["customers"], "last_pk": 99}
        r1 = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        r2 = compute_run_id(checkpoint_before=other, table_deltas=self.deltas)
        assert r1 != r2

    def test_changes_when_row_order_changes(self) -> None:
        # Row order is part of the contract -- divergence is intentional
        # so a non-deterministic ordering bug fails the test loudly.
        flipped = {**self.deltas, "cases": list(reversed(self.deltas["cases"]))}
        a = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        b = compute_run_id(checkpoint_before=self.ckpt, table_deltas=flipped)
        assert a != b

    def test_zero_delta_well_defined(self) -> None:
        empty_ck = Checkpoint.initial(["customers", "cases"]).to_dict()
        a = compute_run_id(checkpoint_before=empty_ck, table_deltas={"customers": [], "cases": []})
        b = compute_run_id(checkpoint_before=empty_ck, table_deltas={"customers": [], "cases": []})
        assert a == b
        assert len(a) == 32

    def test_datetime_in_checkpoint_normalizes(self) -> None:
        ck_dt = {
            "customers": {"updated_at": utc(2026, 3, 31, 12), "last_pk": 30},
            "cases": {"updated_at": utc(2026, 3, 31, 12), "last_pk": 200},
        }
        r_str = compute_run_id(checkpoint_before=self.ckpt, table_deltas=self.deltas)
        r_dt = compute_run_id(checkpoint_before=ck_dt, table_deltas=self.deltas)
        assert r_dt == r_str

    @pytest.mark.parametrize("bad", [4, 65, 0])
    def test_hex_prefix_len_bounds(self, bad: int) -> None:
        with pytest.raises(ValueError):
            compute_run_id(
                checkpoint_before=self.ckpt,
                table_deltas=self.deltas,
                hex_prefix_len=bad,
            )


# ------------------------------------------------------ schema fingerprint


class TestFingerprint:
    def test_deterministic_per_table(self) -> None:
        assert compute_schema_fingerprint(CUSTOMERS) == compute_schema_fingerprint(CUSTOMERS)

    def test_distinct_for_different_tables(self) -> None:
        assert compute_schema_fingerprint(CUSTOMERS) != compute_schema_fingerprint(CASES)

    def test_changes_on_column_add(self) -> None:
        plus = TableSpec(
            name=CUSTOMERS.name,
            primary_key=CUSTOMERS.primary_key,
            columns=CUSTOMERS.columns + (Column("phone", "text"),),
        )
        assert compute_schema_fingerprint(plus) != compute_schema_fingerprint(CUSTOMERS)

    def test_changes_on_type_change(self) -> None:
        renamed = TableSpec(
            name=CUSTOMERS.name,
            primary_key=CUSTOMERS.primary_key,
            columns=tuple(
                Column(c.name, "varchar" if c.name == "name" else c.pg_type)
                for c in CUSTOMERS.columns
            ),
        )
        assert compute_schema_fingerprint(renamed) != compute_schema_fingerprint(CUSTOMERS)


# --------------------------------------------------- checkpoint advancement


class TestAdvanceCheckpoint:
    def test_advances_to_last_row_per_table(self) -> None:
        cp0 = Checkpoint.initial(["customers", "cases"])
        deltas = {
            "customers": [
                {"customer_id": 1, "name": "a", "email": "a", "country": "US", "updated_at": utc(2026, 3, 1)},
                {"customer_id": 2, "name": "b", "email": "b", "country": "US", "updated_at": utc(2026, 3, 2)},
            ],
            "cases": [
                {"case_id": 5, "customer_id": 1, "title": "t", "description": "d", "status": "open", "updated_at": utc(2026, 3, 2)},
            ],
        }
        cp = advance_checkpoint(cp0, deltas, SPECS)
        assert cp.watermark("customers") == TableWatermark(updated_at=utc(2026, 3, 2), last_pk=2)
        assert cp.watermark("cases") == TableWatermark(updated_at=utc(2026, 3, 2), last_pk=5)

    def test_empty_delta_preserves_watermark(self) -> None:
        cp = Checkpoint(per_table={
            "customers": TableWatermark(utc(2026, 3, 1), 30),
            "cases": TableWatermark(utc(2026, 3, 1), 200),
        })
        cp_after = advance_checkpoint(cp, {"customers": [], "cases": []}, SPECS)
        assert cp_after == cp

    def test_first_advance_from_empty_initial(self) -> None:
        cp0 = Checkpoint.initial(["customers", "cases"])
        cp = advance_checkpoint(
            cp0,
            {
                "customers": [
                    {"customer_id": 1, "name": "x", "email": "x", "country": "US", "updated_at": utc(2026, 3, 1)},
                ],
                "cases": [],
            },
            SPECS,
        )
        assert cp.watermark("customers").last_pk == 1
        assert cp.watermark("cases").updated_at == EMPTY_UPDATED_AT


# ---------------------------------------------------- row identity builder


class TestBuildRowIdentities:
    def test_preserves_input_order(self) -> None:
        deltas = {
            "customers": [
                {"customer_id": 2, "name": "b", "email": "b", "country": "US", "updated_at": utc(2026, 3, 2)},
                {"customer_id": 1, "name": "a", "email": "a", "country": "US", "updated_at": utc(2026, 3, 1)},
            ],
            "cases": [],
        }
        ids = build_row_identities(deltas, SPECS)
        # Order is preserved, even if not technically (updated_at, pk)-sorted --
        # the orchestrator relies on the repository's ordering. This test
        # protects against a refactor that silently reorders.
        assert [i.pk for i in ids["customers"]] == [2, 1]
        assert ids["cases"] == []
