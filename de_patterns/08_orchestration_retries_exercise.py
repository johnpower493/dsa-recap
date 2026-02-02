"""Exercise: orchestration retries + idempotent loads.

Goal
----
Orchestrators (Airflow/Dagster/etc.) retry tasks. If a task partially writes results
and then fails, a retry can cause duplicates unless the load is idempotent.

Implement a tiny simulation:
- A source emits batches with (batch_id, rows)
- A sink stores rows
- A state store tracks which batch_ids have been committed

You must implement load_batch_idempotent() so that:
- If the same batch is loaded twice, the sink does not double-apply rows.
- If a failure happens after some writes but before commit, retry must converge
  to exactly-once final state.

Run:
  python de_patterns/08_orchestration_retries_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple


@dataclass(frozen=True)
class Row:
    row_id: str
    payload: dict


class InMemorySink:
    def __init__(self) -> None:
        self.rows_by_id: Dict[str, Row] = {}

    def upsert(self, rows: List[Row]) -> None:
        # upsert by row_id
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
    """Idempotently load a batch.

    Requirements:
    - If batch_id is already committed, do nothing.
    - Otherwise upsert rows into sink.
    - If fail_after_upsert is True, simulate a crash AFTER writing to sink but BEFORE
      recording commit (raise RuntimeError).
    - If it doesn't fail, record batch_id in committed_batches.

    This models the common pattern:
      1) write results (ideally idempotently)
      2) atomically mark checkpoint/watermark

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    sink = InMemorySink()
    state = StateStore()

    batch_id = "2026-01-10T00"
    rows = [Row("r1", {"v": 1}), Row("r2", {"v": 2})]

    # First attempt fails after writing.
    try:
        load_batch_idempotent(sink, state, batch_id, rows, fail_after_upsert=True)
    except RuntimeError:
        pass

    # Retry should converge without duplicates.
    load_batch_idempotent(sink, state, batch_id, rows, fail_after_upsert=False)

    assert set(sink.rows_by_id.keys()) == {"r1", "r2"}
    assert batch_id in state.committed_batches

    # A second retry should be a no-op.
    load_batch_idempotent(sink, state, batch_id, rows, fail_after_upsert=False)
    assert set(sink.rows_by_id.keys()) == {"r1", "r2"}

    print("OK")


if __name__ == "__main__":
    run()
