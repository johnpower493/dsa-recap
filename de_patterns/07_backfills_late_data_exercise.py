"""Exercise: backfills & late-arriving data.

Goal
----
In many pipelines, events arrive late (event time != ingestion time). A common pattern is:
- build aggregates incrementally
- but reprocess a rolling window (e.g., last N days) to capture late data

Implement functions to:
1) Decide which partitions/days to rebuild given:
   - current run date
   - configured lookback window
   - observed late events
2) Generate a plan of days to rebuild.

Run:
  python de_patterns/07_backfills_late_data_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Set


@dataclass(frozen=True)
class LateEvent:
    event_day: date       # event time (partition key)
    ingested_day: date    # when we saw it


def partitions_to_rebuild(
    run_day: date, lookback_days: int, late_events: Iterable[LateEvent]
) -> List[date]:
    """Return sorted list of partition days to rebuild.

    Requirements:
    - Always include the rolling window: [run_day - lookback_days, run_day] inclusive.
    - Also include any event_day from late_events where ingested_day == run_day
      (i.e., late data discovered today), even if outside the rolling window.
    - Do not include future dates.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    run_day = date(2026, 1, 10)
    late_events = [
        LateEvent(event_day=date(2026, 1, 9), ingested_day=run_day),
        LateEvent(event_day=date(2026, 1, 1), ingested_day=run_day),  # very late
        LateEvent(event_day=date(2026, 1, 8), ingested_day=date(2026, 1, 9)),
    ]

    days = partitions_to_rebuild(run_day, lookback_days=2, late_events=late_events)

    # rolling window: 8,9,10 plus very late 1
    assert days == [date(2026, 1, 1), date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 10)], days

    print("OK")


if __name__ == "__main__":
    run()
