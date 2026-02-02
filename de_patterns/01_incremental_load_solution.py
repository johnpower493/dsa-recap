"""Solution: incremental loading with a watermark."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List


@dataclass(frozen=True)
class Event:
    event_ts: datetime
    source_event_id: str
    payload: dict


def incremental_extract(events: Iterable[Event], last_loaded_ts: datetime) -> List[Event]:
    return [e for e in events if e.event_ts > last_loaded_ts]


def advance_watermark(extracted: Iterable[Event], last_loaded_ts: datetime) -> datetime:
    max_ts = last_loaded_ts
    for e in extracted:
        if e.event_ts > max_ts:
            max_ts = e.event_ts
    return max_ts


def _smoke_test() -> None:
    from datetime import timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = [
        Event(base, "e1", {"k": 1}),
        Event(base.replace(hour=1), "e2", {"k": 2}),
        Event(base.replace(hour=2), "e3", {"k": 3}),
        Event(base.replace(hour=2), "e4", {"k": 4}),
    ]

    extracted = incremental_extract(events, base.replace(hour=1))
    assert [e.source_event_id for e in extracted] == ["e3", "e4"]

    wm = advance_watermark(extracted, base.replace(hour=1))
    assert wm == base.replace(hour=2)


if __name__ == "__main__":
    _smoke_test()
    print("OK")
