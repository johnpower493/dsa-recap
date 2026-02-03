"""Exercise: incremental load watermark with tie-breaker (ts + sequence).

Goal
----
A timestamp-only watermark can be unsafe when multiple events share the same event_ts.
Common fix: use a composite watermark (event_ts, sequence) where sequence is
monotonic within a timestamp (or globally).

Implement:
- extract_new(): returns events strictly newer than watermark (ts, seq)
- advance_watermark(): returns max (ts, seq) from extracted

Ordering
--------
Watermark comparison is lexicographic: (ts, seq).
An event is new if (event.ts, event.seq) > watermark.

Run:
  python de_patterns/11_dedup_watermark_ts_seq_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class Event:
    ts: datetime
    seq: int
    event_id: str


Watermark = Tuple[datetime, int]


def extract_new(events: Iterable[Event], watermark: Watermark) -> List[Event]:
    """Return events strictly newer than watermark.

    Requirements:
    - Must be deterministic: sort by (ts, seq) ascending.
    - Must include all events after the watermark.

    TODO: implement.
    """
    raise NotImplementedError


def advance_watermark(extracted: Iterable[Event], watermark: Watermark) -> Watermark:
    """Advance watermark to the max (ts, seq) in extracted, else keep unchanged.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    events = [
        Event(base, 1, "e1"),
        Event(base, 2, "e2"),
        Event(base, 3, "e3"),
        Event(base.replace(hour=1), 1, "e4"),
    ]

    watermark: Watermark = (base, 2)

    extracted = extract_new(events, watermark)
    assert [e.event_id for e in extracted] == ["e3", "e4"], extracted

    wm2 = advance_watermark(extracted, watermark)
    assert wm2 == (base.replace(hour=1), 1)

    # Re-run extraction with advanced watermark yields nothing.
    assert extract_new(events, wm2) == []

    print("OK")


if __name__ == "__main__":
    run()
