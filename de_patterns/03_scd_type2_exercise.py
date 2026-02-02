"""Exercise: SCD Type 2 (dimension history) in Python.

Goal
----
Implement a function that applies attribute changes over time to a Type 2 dimension.

We represent a dimension row with:
- natural_key: stable business identifier (e.g., customer email)
- valid_from / valid_to: time range where attributes are valid
- is_current
- attributes (dict)

Run:
  python de_patterns/03_scd_type2_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List


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


def apply_scd2(existing: List[DimRow], changes: Iterable[Change]) -> List[DimRow]:
    """Apply changes to an existing SCD2 table.

    Requirements:
    - Changes may introduce new natural keys.
    - Only create a new version row if attrs differ from current attrs.
    - When creating a new version:
        - expire current row: set valid_to = effective_at, is_current = False
        - insert new row: valid_from = effective_at, valid_to = None, is_current = True
    - If multiple changes for a key, apply them in effective_at order.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 10, tzinfo=timezone.utc)

    existing = [
        DimRow("ava@example.com", t0, None, True, {"full_name": "Ava Patel", "tier": "free"}),
    ]

    changes = [
        Change("ava@example.com", t1, {"full_name": "Ava Patel", "tier": "pro"}),
        Change("ava@example.com", t2, {"full_name": "Ava P.", "tier": "pro"}),
        Change("ben@example.com", t2, {"full_name": "Ben Kim", "tier": "free"}),
    ]

    out = apply_scd2(existing, changes)

    # Ava should have 3 versions: [t0..t1), [t1..t2), [t2..)
    ava = [r for r in out if r.natural_key == "ava@example.com"]
    assert len(ava) == 3, ava
    assert ava[0].valid_from == t0 and ava[0].valid_to == t1 and not ava[0].is_current
    assert ava[1].valid_from == t1 and ava[1].valid_to == t2 and not ava[1].is_current
    assert ava[2].valid_from == t2 and ava[2].valid_to is None and ava[2].is_current

    # Ben should have 1 current version
    ben = [r for r in out if r.natural_key == "ben@example.com"]
    assert len(ben) == 1 and ben[0].is_current

    print("OK")


if __name__ == "__main__":
    run()
