"""Postgres connection management.

A tiny indirection on top of ``psycopg.connect`` so the repository can take
a *factory* rather than a hard dependency. Tests substitute a fake factory;
production passes the real one wired from settings.

Connections are short-lived and dedicated to a single ingest run. We do not
use a pool because:
    - this is a single-instance prototype;
    - the spec assumes no concurrent /ingest calls (we enforce single-flight
      with a file lock anyway);
    - a fresh connection per run gives us a fresh snapshot for free.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg import Connection


class PgConnectionFactory:
    """Open a single Postgres connection per call."""

    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("dsn must be non-empty")
        self._dsn = dsn

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        conn = psycopg.connect(self._dsn, autocommit=False)
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                # connection was already closed by an earlier error path
                pass
