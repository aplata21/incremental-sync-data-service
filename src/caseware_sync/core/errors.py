"""Domain-level error types.

Kept narrow on purpose: one error per *category of failure mode* so the
api layer can map them to HTTP status codes without leaking internals.
"""

from __future__ import annotations


class CasewareSyncError(Exception):
    """Base class for all service-side errors."""


class IngestInProgressError(CasewareSyncError):
    """Another ingest run is already holding the single-flight lock."""


class CheckpointCorruptError(CasewareSyncError):
    """./state/checkpoint.json exists but is unparseable or schema-invalid."""


class CommitInterruptedError(CasewareSyncError):
    """A staged run was found mid-commit on startup; resume failed."""


class SourceQueryError(CasewareSyncError):
    """Postgres returned an error during the incremental delta query."""
