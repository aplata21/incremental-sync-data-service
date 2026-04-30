"""Incremental source reader.

This is the *only* place the spec's composite-watermark predicate is
expressed in SQL:

    WHERE updated_at > :updated_at
       OR (updated_at = :updated_at AND <pk> > :last_pk)
    ORDER BY updated_at ASC, <pk> ASC

Identifier quoting via ``psycopg.sql.Identifier`` keeps the predicate safe
against SQL injection on ``TableSpec`` fields, even though those values
come from a trusted in-process source. Belt and suspenders.

Transactional behavior
    Every read session opens a single ``REPEATABLE READ READ ONLY``
    transaction. The spec assumes the source does not change during a
    single /ingest run; setting REPEATABLE READ explicitly turns that
    documented assumption into an enforced one — if a concurrent committer
    sneaks in despite the assumption, both tables still observe a single
    consistent snapshot. READ ONLY is a defense-in-depth guarantee that
    this code path can never accidentally write to source.

Streaming
    A *named* (server-side) cursor is used so a multi-million-row delta
    cannot blow up the service's memory. Rows arrive in batches of
    ``fetch_size``. The orchestrator materializes them into a list because
    the run_id needs the full ordered identity list anyway, but anything
    bigger could pivot to a streaming pipeline without changing this layer.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol

import psycopg
from psycopg import Connection, sql
from psycopg.rows import dict_row

from ..core.errors import SourceQueryError
from ..domain.checkpoint import TableWatermark
from ..domain.tables import TableSpec
from .pg import PgConnectionFactory


class SourceReader(Protocol):
    """Interface seam so the orchestrator can be unit-tested with a fake."""

    def fetch_delta(
        self, table: TableSpec, watermark: TableWatermark
    ) -> Iterator[dict[str, Any]]: ...


class IncrementalSourceRepository:
    """Postgres-backed implementation of incremental delta reads."""

    def __init__(
        self,
        conn_factory: PgConnectionFactory,
        *,
        fetch_size: int = 1000,
    ) -> None:
        if fetch_size <= 0:
            raise ValueError("fetch_size must be > 0")
        self._cf = conn_factory
        self._fetch_size = fetch_size

    @contextmanager
    def read_session(self) -> Iterator[_SessionReader]:
        """Open a snapshot read session.

        All ``fetch_delta`` calls inside the ``with`` block see the same
        consistent view of the database. Commits and rollbacks here only
        terminate the read transaction; nothing is ever written.
        """
        with self._cf.connect() as conn:
            try:
                conn.autocommit = False
                conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                conn.read_only = True
                yield _SessionReader(conn, self._fetch_size)
                # End the read tx cleanly so the server releases snapshot resources.
                conn.commit()
            except psycopg.Error as e:
                try:
                    conn.rollback()
                except psycopg.Error:
                    pass
                raise SourceQueryError(str(e)) from e
            except Exception:
                try:
                    conn.rollback()
                except psycopg.Error:
                    pass
                raise


def build_delta_query(table: TableSpec) -> sql.Composed:
    """Compose the parameterized incremental query for a given table.

    Exposed at module level (rather than buried inside ``_SessionReader``)
    so it can be unit-tested without a live connection. The returned object
    is a ``psycopg.sql.Composed`` and renders to the exact SQL the
    repository will execute.
    """
    cols = sql.SQL(", ").join(sql.Identifier(c.name) for c in table.columns)
    tbl = sql.Identifier(table.name)
    pk = sql.Identifier(table.primary_key)
    return sql.SQL(
        "SELECT {cols} FROM {tbl} "
        "WHERE updated_at > %(updated_at)s "
        "OR (updated_at = %(updated_at)s AND {pk} > %(last_pk)s) "
        "ORDER BY updated_at ASC, {pk} ASC"
    ).format(cols=cols, tbl=tbl, pk=pk)


class _SessionReader:
    """Reader scoped to a single read transaction."""

    def __init__(self, conn: Connection, fetch_size: int) -> None:
        self._conn = conn
        self._fetch_size = fetch_size

    def fetch_delta(
        self, table: TableSpec, watermark: TableWatermark
    ) -> Iterator[dict[str, Any]]:
        """Yield rows matching the composite watermark, ordered.

        Yields *plain dicts* keyed by column name (psycopg ``dict_row``).
        Datetime columns come back tz-aware; BIGINT as ``int``; TEXT as
        ``str``. The orchestrator/writers consume these as-is.
        """
        query = build_delta_query(table)
        # A *named* cursor turns this into a server-side cursor; rows arrive
        # in batches of ``itersize`` and the client stays memory-bounded.
        cursor_name = f"delta_{table.name}"
        try:
            with self._conn.cursor(name=cursor_name, row_factory=dict_row) as cur:
                cur.itersize = self._fetch_size
                cur.execute(
                    query,
                    {
                        "updated_at": watermark.updated_at,
                        "last_pk": watermark.last_pk,
                    },
                )
                yield from cur
        except psycopg.Error as e:
            raise SourceQueryError(
                f"failed to fetch delta for {table.name}: {e}"
            ) from e
