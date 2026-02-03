"""Exercise: exactly-once delivery using the transactional outbox pattern.

Goal
----
Implement a small simulation of the outbox pattern:
- The producer writes business data and an outbox message in ONE "transaction".
- A separate publisher reads unpublished outbox messages and publishes them.
- Publishing may fail; the publisher will retry.

Key idea
--------
Avoid the dual-write problem (DB write + message publish). With an outbox, you make
DB the source of truth and publish asynchronously.

In real systems:
- DB transaction ensures atomicity (business row + outbox row)
- publisher marks outbox row published (or stores published_at)
- consumer uses idempotency key / dedup on message_id

Run:
  python de_patterns/10_exactly_once_outbox_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set


@dataclass(frozen=True)
class BusinessRow:
    order_id: str
    amount_cents: int


@dataclass
class OutboxMessage:
    message_id: str
    created_at: datetime
    payload: dict
    published_at: Optional[datetime] = None


class InMemoryDB:
    def __init__(self) -> None:
        self.orders: Dict[str, BusinessRow] = {}
        self.outbox: Dict[str, OutboxMessage] = {}


class DownstreamTopic:
    def __init__(self) -> None:
        # consumer-side dedup using message_id
        self.seen_message_ids: Set[str] = set()
        self.messages: List[dict] = []

    def publish(self, message_id: str, payload: dict, fail: bool) -> None:
        if fail:
            raise RuntimeError("simulated publish failure")

        # downstream dedup
        if message_id in self.seen_message_ids:
            return
        self.seen_message_ids.add(message_id)
        self.messages.append(payload)


def write_order_with_outbox(db: InMemoryDB, order: BusinessRow) -> str:
    """Write business row AND outbox message atomically.

    Requirements:
    - Insert/update the order in db.orders
    - Create an outbox message with a deterministic message_id (e.g., f"order:{order_id}")
    - Overwrite existing outbox message with same message_id is OK (idempotent producer)

    Return the message_id.

    TODO: implement.
    """
    raise NotImplementedError


def publish_outbox(db: InMemoryDB, topic: DownstreamTopic, now: datetime, fail_first: bool) -> None:
    """Publish all unpublished outbox messages.

    Requirements:
    - Iterate messages in created_at order.
    - For each message with published_at is None:
        - call topic.publish(message_id, payload, fail=...)
        - if publish succeeds, set published_at = now
        - if publish fails, stop (simulate crash) WITHOUT marking published
    - fail_first controls whether the first publish attempt fails.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    db = InMemoryDB()
    topic = DownstreamTopic()

    msg_id = write_order_with_outbox(db, BusinessRow("o-1", 2999))
    assert msg_id in db.outbox

    # First publisher run fails before marking published.
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        publish_outbox(db, topic, now=t1, fail_first=True)
    except RuntimeError:
        pass

    # Retry: should publish exactly once.
    t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    publish_outbox(db, topic, now=t2, fail_first=False)

    assert len(topic.messages) == 1
    assert db.outbox[msg_id].published_at == t2

    # Re-run publisher: no new messages.
    publish_outbox(db, topic, now=t2, fail_first=False)
    assert len(topic.messages) == 1

    print("OK")


if __name__ == "__main__":
    run()
