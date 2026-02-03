"""Solution: exactly-once delivery using transactional outbox pattern."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
        self.seen_message_ids: Set[str] = set()
        self.messages: List[dict] = []

    def publish(self, message_id: str, payload: dict, fail: bool) -> None:
        if fail:
            raise RuntimeError("simulated publish failure")
        if message_id in self.seen_message_ids:
            return
        self.seen_message_ids.add(message_id)
        self.messages.append(payload)


def write_order_with_outbox(db: InMemoryDB, order: BusinessRow) -> str:
    # Deterministic message id makes producer idempotent.
    message_id = f"order:{order.order_id}"

    # "Transaction": update business row and outbox row.
    db.orders[order.order_id] = order

    now = datetime.utcnow()  # only used for ordering within this simulation
    db.outbox[message_id] = OutboxMessage(
        message_id=message_id,
        created_at=now,
        payload={"order_id": order.order_id, "amount_cents": order.amount_cents},
        published_at=db.outbox.get(message_id).published_at if message_id in db.outbox else None,
    )

    return message_id


def publish_outbox(db: InMemoryDB, topic: DownstreamTopic, now: datetime, fail_first: bool) -> None:
    # stable order
    msgs = sorted(db.outbox.values(), key=lambda m: (m.created_at, m.message_id))

    first = True
    for m in msgs:
        if m.published_at is not None:
            continue

        topic.publish(m.message_id, m.payload, fail=(fail_first and first))
        first = False

        # mark published after successful publish
        m.published_at = now


def _smoke_test() -> None:
    from datetime import timezone

    db = InMemoryDB()
    topic = DownstreamTopic()

    msg_id = write_order_with_outbox(db, BusinessRow("o-1", 2999))

    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        publish_outbox(db, topic, t1, True)
    except RuntimeError:
        pass

    t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    publish_outbox(db, topic, t2, False)

    assert len(topic.messages) == 1
    assert db.outbox[msg_id].published_at == t2

    publish_outbox(db, topic, t2, False)
    assert len(topic.messages) == 1


if __name__ == "__main__":
    _smoke_test()
    print("OK")
