"""Solution: schema evolution + backward/forward compatibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CanonicalEvent:
    customer_id: str
    occurred_at: str
    tier: str
    country: Optional[str]


def _normalize_ts(ts: str) -> str:
    # Normalize to UTC Z. Keep it simple but robust for common ISO forms.
    # Accept strings like 2026-01-01T00:00:00Z or with +00:00.
    s = ts.strip()
    if s.endswith("Z"):
        # validate parse
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return s

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_event(payload: Dict[str, Any]) -> CanonicalEvent:
    version = int(payload.get("schema_version", 1))

    if version == 1:
        customer_id = payload["user_id"]
        occurred_at = _normalize_ts(payload["event_ts"])
        props = payload.get("properties") or {}
        tier = props.get("plan") or "free"
        country = None
        return CanonicalEvent(customer_id=customer_id, occurred_at=occurred_at, tier=tier, country=country)

    if version == 2:
        customer_id = payload["customer_id"]
        occurred_at = _normalize_ts(payload["occurred_at"])
        props = payload.get("properties") or {}
        tier = props.get("tier") or "free"
        country = props.get("country")
        return CanonicalEvent(customer_id=customer_id, occurred_at=occurred_at, tier=tier, country=country)

    # Forward-compat approach: attempt best-effort mapping for unknown versions.
    # Prefer v2 field names, fall back to v1.
    customer_id = payload.get("customer_id") or payload.get("user_id")
    if not customer_id:
        raise KeyError("customer_id/user_id")

    occurred_at_raw = payload.get("occurred_at") or payload.get("event_ts")
    if not occurred_at_raw:
        raise KeyError("occurred_at/event_ts")

    occurred_at = _normalize_ts(str(occurred_at_raw))
    props = payload.get("properties") or {}
    tier = props.get("tier") or props.get("plan") or "free"
    country = props.get("country")

    return CanonicalEvent(customer_id=customer_id, occurred_at=occurred_at, tier=tier, country=country)


def _smoke_test() -> None:
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
    assert e1.customer_id == "u1" and e1.tier == "free" and e1.country is None

    e2 = normalize_event(v2)
    assert e2.customer_id == "u2" and e2.tier == "pro" and e2.country == "US"

    e3 = normalize_event(v2_missing_optional)
    assert e3.tier == "free" and e3.country is None


if __name__ == "__main__":
    _smoke_test()
    print("OK")
