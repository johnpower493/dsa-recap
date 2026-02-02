"""Solution: partitioning strategy selection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Workload:
    table: str
    rows_per_day: int
    retention_days: int
    filter_by_date_range: bool
    filter_by_customer_id: bool
    mostly_append_only: bool


@dataclass(frozen=True)
class Recommendation:
    partition_by: str
    granularity: str
    subpartition_by_hash: Optional[str]
    notes: List[str]


def recommend_partitioning(w: Workload) -> Recommendation:
    notes: List[str] = []

    # Default: partition facts by time.
    partition_by = "event_date" if w.filter_by_date_range else "event_ts"

    # Heuristic granularity.
    # Target something like 50-500 partitions in retention window for manageability.
    daily_partitions = w.retention_days
    monthly_partitions = max(1, (w.retention_days + 29) // 30)

    if w.filter_by_date_range:
        notes.append("RANGE partitioning by date enables partition pruning for time-bounded queries.")

    # Very high volume -> smaller partitions; very long retention -> fewer partitions.
    if w.rows_per_day >= 10_000_000 and daily_partitions <= 400:
        granularity = "day"
        notes.append(
            "Daily partitions keep per-partition indexes smaller and reduce vacuum/maintenance scope."
        )
    else:
        granularity = "month"
        notes.append(
            "Monthly partitions reduce partition count for long retention, simplifying management."
        )

    subpartition_by_hash: Optional[str] = None
    huge_table = w.rows_per_day * w.retention_days >= 1_000_000_000
    if w.filter_by_customer_id and huge_table:
        subpartition_by_hash = "customer_id"
        notes.append(
            "Optional hash subpartitioning by customer_id can improve parallelism and hotspot distribution."
        )

    if not w.mostly_append_only:
        notes.append(
            "Frequent updates/deletes can reduce partitioning benefits; consider clustering/indexing instead."
        )

    notes.append(
        "Tradeoff: too-fine partitions increase planning overhead and create many small indexes/files."
    )

    return Recommendation(
        partition_by=partition_by,
        granularity=granularity,
        subpartition_by_hash=subpartition_by_hash,
        notes=notes,
    )


def _smoke_test() -> None:
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


if __name__ == "__main__":
    _smoke_test()
    print("OK")
