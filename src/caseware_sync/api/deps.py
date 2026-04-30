"""Composition root.

Wires Settings -> concrete dependencies -> orchestrator. Tests can build
a parallel orchestrator pointed at tmp dirs and a docker-compose Postgres
without going through ``init_orchestrator()``.
"""

from __future__ import annotations

from ..core.clock import SystemClock
from ..core.config import Settings, load_settings
from ..core.logging import get_logger
from ..domain.tables import ALL_TABLES
from ..outputs.event_writer import EventWriter
from ..outputs.lake_writer import LakeWriter
from ..outputs.share_writer import ShareWriter
from ..pipeline.commit import Committer
from ..pipeline.orchestrator import IngestRunOrchestrator
from ..source.pg import PgConnectionFactory
from ..source.repository import IncrementalSourceRepository
from ..state.checkpoint_store import CheckpointStore


def build_orchestrator(settings: Settings) -> IngestRunOrchestrator:
    """Construct an orchestrator from the given settings.

    Pulled out as a free function so tests can call it with a custom
    ``Settings(...)`` (e.g. tmp dirs, an alternate DSN).
    """
    conn_factory = PgConnectionFactory(str(settings.database_url))
    repository = IncrementalSourceRepository(conn_factory)
    checkpoint_store = CheckpointStore(
        settings.checkpoint_path, [t.name for t in ALL_TABLES]
    )
    return IngestRunOrchestrator(
        settings=settings,
        repository=repository,
        checkpoint_store=checkpoint_store,
        lake_writer=LakeWriter(settings.lake_dir),
        share_writer=ShareWriter(settings.share_dir),
        event_writer=EventWriter(settings.events_dir),
        committer=Committer(get_logger("caseware_sync.commit")),
        clock=SystemClock(),
        table_specs=ALL_TABLES,
        run_id_hex_prefix_len=settings.run_id_hex_prefix_len,
        logger=get_logger("caseware_sync.orchestrator"),
    )


def init_orchestrator() -> IngestRunOrchestrator:
    return build_orchestrator(load_settings())
