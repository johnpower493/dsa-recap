"""Exercise: incremental loading with a watermark.

Goal
----
Implement a function that selects only new records since the last successful load,
then advances the watermark.

This pattern is common when ingesting from append-only sources (logs/events) into a warehouse.

Run:
  python de_patterns/01_incremental_load_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class Event:
    event_ts: datetime
    source_event_id: str
    payload: dict


def incremental_extract(events: Iterable[Event], last_loaded_ts: datetime) -> List[Event]:
    """Return events strictly newer than last_loaded_ts.

    Requirements:
    - Must be stable/deterministic (preserve original order for equal timestamps).
    - Must not mutate the input.
    - Use strict '>' (events at exactly last_loaded_ts are considered already loaded).

    TODO: implement.
    """
    raise NotImplementedError


def advance_watermark(extracted: Iterable[Event], last_loaded_ts: datetime) -> datetime:
    """Return the new watermark after a successful load.

    Requirements:
    - If extracted is empty, return last_loaded_ts unchanged.
    - Otherwise return max(event_ts) among extracted.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> Tuple[List[Event], datetime]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = [
        Event(base, "e1", {"k": 1}),
        Event(base.replace(hour=1), "e2", {"k": 2}),
        Event(base.replace(hour=2), "e3", {"k": 3}),
        # duplicate timestamp; order should be preserved
        Event(base.replace(hour=2), "e4", {"k": 4}),
    ]

    last_loaded_ts = base.replace(hour=1)

    extracted = incremental_extract(events, last_loaded_ts)
    new_watermark = advance_watermark(extracted, last_loaded_ts)

    # Expected: e3, e4
    assert [e.source_event_id for e in extracted] == ["e3", "e4"], extracted
    assert new_watermark == base.replace(hour=2)

    return extracted, new_watermark


if __name__ == "__main__":
    extracted, watermark = run()
    print("OK")
    print("extracted:", [e.source_event_id for e in extracted])
    print("watermark:", watermark.isoformat())
