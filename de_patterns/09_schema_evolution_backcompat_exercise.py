"""Exercise: schema evolution + backward/forward compatibility.

Goal
----
Data engineering pipelines frequently ingest semi-structured events (JSON). Schemas evolve:
- fields are added
- fields are renamed
- types change

Implement normalize_event() to support BOTH v1 and v2 payloads and output a canonical record.

Rules for this exercise
-----------------------
Input payloads are dicts with a "schema_version" key.

v1 example:
  {
    "schema_version": 1,
    "user_id": "u1",
    "event_ts": "2026-01-01T00:00:00Z",
    "properties": {"plan": "free"}
  }

v2 changes:
- user_id renamed to customer_id
- event_ts renamed to occurred_at
- properties.plan renamed to properties.tier
- new optional field: properties.country

Back/forward-compat requirements
--------------------------------
- Backward: v2 consumer must be able to read v1.
- Forward-ish: if payload has unknown fields, ignore them.
- If a field is missing, use defaults:
    - tier defaults to "free"
    - country defaults to None
- Parse timestamps into a comparable format (keep as ISO string is OK if normalized).

Run:
  python de_patterns/09_schema_evolution_backcompat_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CanonicalEvent:
    customer_id: str
    occurred_at: str  # ISO-8601 string normalized to Z
    tier: str
    country: Optional[str]


def normalize_event(payload: Dict[str, Any]) -> CanonicalEvent:
    """Normalize v1/v2 payloads into CanonicalEvent.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    v1 = {
        "schema_version": 1,
        "user_id": "u1",
        "event_ts": "2026-01-01T00:00:00Z",
        "properties": {"plan": "free"},
        "unknown": {"ignored": True},
    }

    v2 = {
        "schema_version": 2,
        "customer_id": "u2",
        "occurred_at": "2026-01-02T03:04:05Z",
        "properties": {"tier": "pro", "country": "US"},
    }

    v2_missing_optional = {
        "schema_version": 2,
        "customer_id": "u3",
        "occurred_at": "2026-01-03T00:00:00Z",
        "properties": {},
    }

    e1 = normalize_event(v1)
    assert e1.customer_id == "u1"
    assert e1.tier == "free" and e1.country is None

    e2 = normalize_event(v2)
    assert e2.customer_id == "u2" and e2.tier == "pro" and e2.country == "US"

    e3 = normalize_event(v2_missing_optional)
    assert e3.tier == "free" and e3.country is None

    print("OK")


if __name__ == "__main__":
    run()
