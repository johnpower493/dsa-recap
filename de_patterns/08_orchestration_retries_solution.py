"""Solution: orchestration retries + idempotent loads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass(frozen=True)
class Row:
    row_id: str
    payload: dict


class InMemorySink:
    def __init__(self) -> None:
        self.rows_by_id: Dict[str, Row] = {}

    def upsert(self, rows: List[Row]) -> None:
        for r in rows:
            self.rows_by_id[r.row_id] = r


class StateStore:
    def __init__(self) -> None:
        self.committed_batches: Set[str] = set()


def load_batch_idempotent(
    sink: InMemorySink,
    state: StateStore,
    batch_id: str,
    rows: List[Row],
    fail_after_upsert: bool,
) -> None:
    if batch_id in state.committed_batches:
        return

    # Key idea: make the write idempotent (upsert/merge) so repeating it is safe.
    sink.upsert(rows)

    if fail_after_upsert:
        raise RuntimeError("simulated crash after upsert")

    # In real systems, checkpointing should be atomic/durable.
    state.committed_batches.add(batch_id)


def _smoke_test() -> None:
    sink = InMemorySink()
    state = StateStore()

    batch_id = "2026-01-10T00"
    rows = [Row("r1", {"v": 1}), Row("r2", {"v": 2})]

    try:
        load_batch_idempotent(sink, state, batch_id, rows, True)
    except RuntimeError:
        pass

    load_batch_idempotent(sink, state, batch_id, rows, False)
    assert set(sink.rows_by_id.keys()) == {"r1", "r2"}
    assert batch_id in state.committed_batches

    load_batch_idempotent(sink, state, batch_id, rows, False)
    assert set(sink.rows_by_id.keys()) == {"r1", "r2"}


if __name__ == "__main__":
    _smoke_test()
    print("OK")
