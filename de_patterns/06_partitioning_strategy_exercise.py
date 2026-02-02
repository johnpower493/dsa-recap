"""Exercise: partitioning strategy selection.

Goal
----
Given a workload + data characteristics, choose a partitioning strategy:
- partition key (e.g., event_date)
- granularity (daily/monthly)
- optional sub-partitioning (hash by customer_id)

This is a planning exercise: you'll implement simple heuristics that produce a recommendation.

Run:
  python de_patterns/06_partitioning_strategy_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Workload:
    table: str
    rows_per_day: int
    retention_days: int
    # common filters
    filter_by_date_range: bool
    filter_by_customer_id: bool
    # update/delete patterns
    mostly_append_only: bool


@dataclass(frozen=True)
class Recommendation:
    partition_by: str
    granularity: str  # 'day' | 'month'
    subpartition_by_hash: Optional[str]
    notes: List[str]


def recommend_partitioning(w: Workload) -> Recommendation:
    """Return a partitioning recommendation.

    Requirements:
    - Prefer RANGE partitioning by date when queries commonly filter by date range.
    - Choose granularity:
        - daily if very high volume or short retention (many partitions ok)
        - monthly if moderate volume or very long retention
    - If there is frequent filtering by customer_id AND table is huge, suggest hash subpartitioning.
    - Include at least 2 notes explaining tradeoffs (partition pruning, maintenance, small files).

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    w = Workload(
        table="fact_events",
        rows_per_day=50_000_000,
        retention_days=180,
        filter_by_date_range=True,
        filter_by_customer_id=True,
        mostly_append_only=True,
    )

    rec = recommend_partitioning(w)
    assert rec.partition_by in {"event_date", "event_ts"}
    assert rec.granularity in {"day", "month"}
    assert len(rec.notes) >= 2

    print("OK")
    print(rec)


if __name__ == "__main__":
    run()
