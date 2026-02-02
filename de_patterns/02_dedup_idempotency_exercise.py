"""Exercise: deduplication + idempotency keys.

Goal
----
Given a stream of records that may contain duplicates, implement:
  1) dedup_keep_latest: keep only the latest record per key.
  2) build_idempotency_key: generate a stable idempotency key for a record.

This mirrors patterns used for:
- de-duplicating CDC/event streams
- exactly-once-ish semantics on top of at-least-once delivery

Run:
  python de_patterns/02_dedup_idempotency_exercise.py
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Dict, Iterable, List, Tuple


@dataclass(frozen=True)
class Record:
    key: str
    updated_at: datetime
    payload: dict


def dedup_keep_latest(records: Iterable[Record]) -> List[Record]:
    """Keep the latest record per key (by updated_at).

    Requirements:
    - If two records have the same key and updated_at, keep the *last* one seen.
    - Return results sorted by key for testability.

    TODO: implement.
    """
    raise NotImplementedError


def build_idempotency_key(record: Record) -> str:
    """Build a stable idempotency key for a record.

    Requirements:
    - Must be deterministic.
    - Must not depend on dict insertion order.
    - Should change if key/updated_at/payload content changes.

    Hint: canonical JSON + sha256.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> Tuple[List[Record], List[str]]:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    records = [
        Record("A", t0, {"v": 1}),
        Record("A", t1, {"v": 2}),
        Record("B", t0, {"v": 1}),
        # same ts: last one wins
        Record("B", t0, {"v": 99}),
    ]

    deduped = dedup_keep_latest(records)
    assert [(r.key, r.payload["v"]) for r in deduped] == [("A", 2), ("B", 99)], deduped

    keys = [build_idempotency_key(r) for r in deduped]
    assert all(isinstance(k, str) and len(k) == 64 for k in keys), keys
    assert keys[0] != keys[1]

    return deduped, keys


if __name__ == "__main__":
    deduped, keys = run()
    print("OK")
    print("deduped:", [(r.key, r.updated_at.isoformat(), r.payload) for r in deduped])
    print("idempotency_keys:", keys)
