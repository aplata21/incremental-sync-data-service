"""Repository SQL composition.

We assert the SQL we *would* execute matches the spec verbatim. Done with
``psycopg.sql.Composed.as_string`` so we don't need a live DB connection.
"""

from __future__ import annotations

import psycopg

from caseware_sync.domain.tables import CASES, CUSTOMERS
from caseware_sync.source.repository import build_delta_query


def _render(query) -> str:
    """Render a ``Composed`` to a SQL string without a live connection.

    psycopg3 ``as_string`` accepts ``None`` since 3.1 for the simple-cases
    of fully-formed Composed objects.
    """
    return query.as_string(None)


def test_customers_predicate_matches_spec() -> None:
    sql = _render(build_delta_query(CUSTOMERS))
    # Identifier-quoted column list and table.
    assert '"customer_id"' in sql and '"updated_at"' in sql
    assert 'FROM "customers"' in sql
    # Composite watermark predicate, both branches.
    assert "updated_at > %(updated_at)s" in sql
    assert (
        '(updated_at = %(updated_at)s AND "customer_id" > %(last_pk)s)'
        in sql
    )
    # Deterministic ordering.
    assert 'ORDER BY updated_at ASC, "customer_id" ASC' in sql


def test_cases_predicate_matches_spec() -> None:
    sql = _render(build_delta_query(CASES))
    assert '"case_id"' in sql and 'FROM "cases"' in sql
    assert 'AND "case_id" > %(last_pk)s' in sql
    assert 'ORDER BY updated_at ASC, "case_id" ASC' in sql
