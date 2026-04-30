"""Injectable clock so manifest timestamps (started_at / finished_at) are
testable without mocking ``datetime.now`` globally.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now_utc(self) -> datetime: ...


class SystemClock:
    """Default clock used in production. Always returns timezone-aware UTC."""

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock:
    """Test helper. Optionally advances by a fixed delta on each call so a
    single test can assert ``finished_at > started_at``."""

    def __init__(self, start: datetime, *, step_seconds: float = 0.0) -> None:
        if start.tzinfo is None:
            raise ValueError("FixedClock requires a timezone-aware datetime")
        self._t = start.astimezone(timezone.utc)
        self._step = step_seconds

    def now_utc(self) -> datetime:
        current = self._t
        if self._step:
            from datetime import timedelta

            self._t = self._t + timedelta(seconds=self._step)
        return current
