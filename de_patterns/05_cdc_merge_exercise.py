"""Exercise: CDC merge (apply change data capture to a current-state snapshot).

Goal
----
Implement apply_cdc() that takes:
- current snapshot rows (by primary key)
- a list of CDC events (insert/update/delete)

and returns a new snapshot.

This mirrors patterns used to apply:
- Debezium/Kafka CDC streams
- database replication logs
- incremental API snapshots with tombstones

Assumptions
-----------
- Each CDC event has: pk, op in {'I','U','D'}, ts (monotonic per pk), and payload.
- Later events override earlier ones.
- Delete removes the row.

Run:
  python de_patterns/05_cdc_merge_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, MutableMapping


@dataclass(frozen=True)
class Row:
    pk: str
    payload: dict


@dataclass(frozen=True)
class CdcEvent:
    pk: str
    op: str  # 'I' | 'U' | 'D'
    ts: datetime
    payload: dict | None


def apply_cdc(snapshot: Mapping[str, Row], events: Iterable[CdcEvent]) -> Dict[str, Row]:
    """Apply CDC events to a snapshot.

    Requirements:
    - Apply events in (pk, ts) order to be deterministic.
    - 'I' and 'U' both upsert the row using event.payload.
    - 'D' deletes the row.
    - If multiple events have same (pk, ts), keep the last one in input order.
    - Return a NEW dict (do not mutate input snapshot).

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 3, tzinfo=timezone.utc)

    snapshot = {
        "A": Row("A", {"name": "Ava", "tier": "free"}),
        "B": Row("B", {"name": "Ben", "tier": "free"}),
    }

    events: List[CdcEvent] = [
        CdcEvent("A", "U", t1, {"name": "Ava", "tier": "pro"}),
        CdcEvent("C", "I", t1, {"name": "Cora", "tier": "free"}),
        CdcEvent("B", "D", t2, None),
        # same ts, last one wins
        CdcEvent("C", "U", t2, {"name": "Cora", "tier": "pro"}),
        CdcEvent("C", "U", t2, {"name": "Cora J.", "tier": "pro"}),
    ]

    out = apply_cdc(snapshot, events)

    assert set(out.keys()) == {"A", "C"}, out
    assert out["A"].payload["tier"] == "pro"
    assert out["C"].payload["name"] == "Cora J."

    # ensure input snapshot untouched
    assert set(snapshot.keys()) == {"A", "B"}

    print("OK")


if __name__ == "__main__":
    run()
