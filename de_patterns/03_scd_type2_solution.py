"""Solution: SCD Type 2 (dimension history) in Python."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Tuple


@dataclass
class DimRow:
    natural_key: str
    valid_from: datetime
    valid_to: datetime | None
    is_current: bool
    attrs: Dict[str, str]


@dataclass(frozen=True)
class Change:
    natural_key: str
    effective_at: datetime
    attrs: Dict[str, str]


def _index_current(rows: List[DimRow]) -> Dict[str, DimRow]:
    cur: Dict[str, DimRow] = {}
    for r in rows:
        if r.is_current:
            cur[r.natural_key] = r
    return cur


def apply_scd2(existing: List[DimRow], changes: Iterable[Change]) -> List[DimRow]:
    out = list(existing)
    current_by_key = _index_current(out)

    # Sort changes by (key, effective_at) to ensure deterministic application.
    ordered: List[Change] = sorted(changes, key=lambda c: (c.natural_key, c.effective_at))

    for ch in ordered:
        cur = current_by_key.get(ch.natural_key)

        if cur is None:
            new_row = DimRow(
                natural_key=ch.natural_key,
                valid_from=ch.effective_at,
                valid_to=None,
                is_current=True,
                attrs=dict(ch.attrs),
            )
            out.append(new_row)
            current_by_key[ch.natural_key] = new_row
            continue

        if cur.attrs == ch.attrs:
            continue

        # Expire current and insert new.
        cur.valid_to = ch.effective_at
        cur.is_current = False

        new_row = DimRow(
            natural_key=ch.natural_key,
            valid_from=ch.effective_at,
            valid_to=None,
            is_current=True,
            attrs=dict(ch.attrs),
        )
        out.append(new_row)
        current_by_key[ch.natural_key] = new_row

    return out


def _smoke_test() -> None:
    from datetime import timezone

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 10, tzinfo=timezone.utc)

    existing = [DimRow("ava@example.com", t0, None, True, {"full_name": "Ava Patel", "tier": "free"})]
    changes = [
        Change("ava@example.com", t1, {"full_name": "Ava Patel", "tier": "pro"}),
        Change("ava@example.com", t2, {"full_name": "Ava P.", "tier": "pro"}),
        Change("ben@example.com", t2, {"full_name": "Ben Kim", "tier": "free"}),
    ]

    out = apply_scd2(existing, changes)

    ava = [r for r in out if r.natural_key == "ava@example.com"]
    assert len(ava) == 3
    assert ava[0].valid_from == t0 and ava[0].valid_to == t1 and not ava[0].is_current
    assert ava[1].valid_from == t1 and ava[1].valid_to == t2 and not ava[1].is_current
    assert ava[2].valid_from == t2 and ava[2].valid_to is None and ava[2].is_current

    ben = [r for r in out if r.natural_key == "ben@example.com"]
    assert len(ben) == 1 and ben[0].is_current


if __name__ == "__main__":
    _smoke_test()
    print("OK")
