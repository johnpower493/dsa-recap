"""Solution: backfills & late-arriving data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Set


@dataclass(frozen=True)
class LateEvent:
    event_day: date
    ingested_day: date


def partitions_to_rebuild(
    run_day: date, lookback_days: int, late_events: Iterable[LateEvent]
) -> List[date]:
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")

    days: Set[date] = set()

    # Rolling window inclusive.
    start = run_day - timedelta(days=lookback_days)
    d = start
    while d <= run_day:
        days.add(d)
        d += timedelta(days=1)

    # Late arrivals discovered today.
    for ev in late_events:
        if ev.ingested_day == run_day and ev.event_day <= run_day:
            days.add(ev.event_day)

    return sorted(days)


def _smoke_test() -> None:
    run_day = date(2026, 1, 10)
    late_events = [
        LateEvent(event_day=date(2026, 1, 9), ingested_day=run_day),
        LateEvent(event_day=date(2026, 1, 1), ingested_day=run_day),
        LateEvent(event_day=date(2026, 1, 8), ingested_day=date(2026, 1, 9)),
    ]
    days = partitions_to_rebuild(run_day, 2, late_events)
    assert days == [date(2026, 1, 1), date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 10)]


if __name__ == "__main__":
    _smoke_test()
    print("OK")
