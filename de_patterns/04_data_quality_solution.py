"""Solution: data quality checks (Python implementation)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Set


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    failing_examples: List[dict]


def check_unique(rows: Sequence[dict], column: str, max_examples: int = 5) -> CheckResult:
    counts = {}
    for r in rows:
        v = r.get(column)
        counts[v] = counts.get(v, 0) + 1

    failing = []
    for r in rows:
        v = r.get(column)
        if counts.get(v, 0) > 1:
            failing.append(r)
            if len(failing) >= max_examples:
                break

    return CheckResult(
        name=f"unique({column})",
        passed=len(failing) == 0,
        failing_examples=failing,
    )


def check_not_null(rows: Sequence[dict], column: str, max_examples: int = 5) -> CheckResult:
    failing = []
    for r in rows:
        if r.get(column) is None:
            failing.append(r)
            if len(failing) >= max_examples:
                break

    return CheckResult(
        name=f"not_null({column})",
        passed=len(failing) == 0,
        failing_examples=failing,
    )


def check_accepted_values(
    rows: Sequence[dict], column: str, allowed: Set[object], max_examples: int = 5
) -> CheckResult:
    failing = []
    for r in rows:
        v = r.get(column)
        if v not in allowed:
            failing.append(r)
            if len(failing) >= max_examples:
                break

    return CheckResult(
        name=f"accepted_values({column})",
        passed=len(failing) == 0,
        failing_examples=failing,
    )


def _smoke_test() -> None:
    rows = [
        {"id": 1, "event_type": "purchase", "amount": 10},
        {"id": 2, "event_type": "page_view", "amount": None},
        {"id": 2, "event_type": "refund", "amount": -10},
        {"id": 3, "event_type": "oops", "amount": 0},
    ]

    assert check_unique(rows, "id").passed is False
    assert check_not_null(rows, "event_type").passed is True
    assert check_not_null(rows, "amount").passed is False
    assert check_accepted_values(rows, "event_type", {"purchase", "refund", "page_view"}).passed is False


if __name__ == "__main__":
    _smoke_test()
    print("OK")
