"""Solution: incremental load watermark with tie-breaker (ts + seq)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class Event:
    ts: datetime
    seq: int
    event_id: str


Watermark = Tuple[datetime, int]


def extract_new(events: Iterable[Event], watermark: Watermark) -> List[Event]:
    ordered = sorted(events, key=lambda e: (e.ts, e.seq, e.event_id))
    w_ts, w_seq = watermark
    return [e for e in ordered if (e.ts, e.seq) > (w_ts, w_seq)]


def advance_watermark(extracted: Iterable[Event], watermark: Watermark) -> Watermark:
    w_ts, w_seq = watermark
    max_w = (w_ts, w_seq)
    for e in extracted:
        if (e.ts, e.seq) > max_w:
            max_w = (e.ts, e.seq)
    return max_w


def _smoke_test() -> None:
    from datetime import timezone

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = [
        Event(base, 1, "e1"),
        Event(base, 2, "e2"),
        Event(base, 3, "e3"),
        Event(base.replace(hour=1), 1, "e4"),
    ]

    watermark: Watermark = (base, 2)
    extracted = extract_new(events, watermark)
    assert [e.event_id for e in extracted] == ["e3", "e4"]

    wm2 = advance_watermark(extracted, watermark)
    assert wm2 == (base.replace(hour=1), 1)
    assert extract_new(events, wm2) == []


if __name__ == "__main__":
    _smoke_test()
    print("OK")
