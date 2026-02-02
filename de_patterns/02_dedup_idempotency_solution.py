"""Solution: deduplication + idempotency keys."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class Record:
    key: str
    updated_at: datetime
    payload: dict


def dedup_keep_latest(records: Iterable[Record]) -> List[Record]:
    latest_by_key: Dict[str, Record] = {}
    for r in records:
        cur = latest_by_key.get(r.key)
        if cur is None:
            latest_by_key[r.key] = r
            continue

        if r.updated_at > cur.updated_at:
            latest_by_key[r.key] = r
        elif r.updated_at == cur.updated_at:
            # last one wins
            latest_by_key[r.key] = r

    return [latest_by_key[k] for k in sorted(latest_by_key.keys())]


def build_idempotency_key(record: Record) -> str:
    canonical = json.dumps(
        {
            "key": record.key,
            "updated_at": record.updated_at.isoformat(),
            "payload": record.payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _smoke_test() -> None:
    from datetime import timezone

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    records = [
        Record("A", t0, {"v": 1}),
        Record("A", t1, {"v": 2}),
        Record("B", t0, {"v": 1}),
        Record("B", t0, {"v": 99}),
    ]

    deduped = dedup_keep_latest(records)
    assert [(r.key, r.payload["v"]) for r in deduped] == [("A", 2), ("B", 99)]

    keys = [build_idempotency_key(r) for r in deduped]
    assert len(keys[0]) == 64 and len(keys[1]) == 64 and keys[0] != keys[1]


if __name__ == "__main__":
    _smoke_test()
    print("OK")
