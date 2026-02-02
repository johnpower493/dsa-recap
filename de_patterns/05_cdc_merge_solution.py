"""Solution: CDC merge (apply CDC to snapshot)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Mapping


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
    out: Dict[str, Row] = dict(snapshot)

    # Stable tie-break: keep last event for identical (pk, ts)
    indexed: List[tuple[int, CdcEvent]] = list(enumerate(events))
    indexed.sort(key=lambda it: (it[1].pk, it[1].ts, it[0]))

    for _, ev in indexed:
        if ev.op not in {"I", "U", "D"}:
            raise ValueError(f"unknown op: {ev.op}")

        if ev.op == "D":
            out.pop(ev.pk, None)
            continue

        if ev.payload is None:
            raise ValueError("payload required for I/U")

        out[ev.pk] = Row(ev.pk, dict(ev.payload))

    return out


def _smoke_test() -> None:
    from datetime import timezone

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 3, tzinfo=timezone.utc)

    snapshot = {
        "A": Row("A", {"name": "Ava", "tier": "free"}),
        "B": Row("B", {"name": "Ben", "tier": "free"}),
    }

    events = [
        CdcEvent("A", "U", t1, {"name": "Ava", "tier": "pro"}),
        CdcEvent("C", "I", t1, {"name": "Cora", "tier": "free"}),
        CdcEvent("B", "D", t2, None),
        CdcEvent("C", "U", t2, {"name": "Cora", "tier": "pro"}),
        CdcEvent("C", "U", t2, {"name": "Cora J.", "tier": "pro"}),
    ]

    out = apply_cdc(snapshot, events)
    assert set(out.keys()) == {"A", "C"}
    assert out["A"].payload["tier"] == "pro"
    assert out["C"].payload["name"] == "Cora J."
    assert set(snapshot.keys()) == {"A", "B"}


if __name__ == "__main__":
    _smoke_test()
    print("OK")
