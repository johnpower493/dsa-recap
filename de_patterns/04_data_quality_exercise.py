"""Exercise: data quality checks (Python implementation).

Goal
----
Implement lightweight checks that mirror common warehouse tests:
- uniqueness
- not-null
- accepted-values

Run:
  python de_patterns/04_data_quality_exercise.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Sequence, Set, Tuple, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    failing_examples: List[dict]


def check_unique(rows: Sequence[dict], column: str, max_examples: int = 5) -> CheckResult:
    """Fail if any value in column appears more than once.

    TODO: implement.
    """
    raise NotImplementedError


def check_not_null(rows: Sequence[dict], column: str, max_examples: int = 5) -> CheckResult:
    """Fail if any row has NULL/None in column.

    TODO: implement.
    """
    raise NotImplementedError


def check_accepted_values(
    rows: Sequence[dict], column: str, allowed: Set[object], max_examples: int = 5
) -> CheckResult:
    """Fail if any value is not in the allowed set.

    TODO: implement.
    """
    raise NotImplementedError


def run() -> None:
    rows = [
        {"id": 1, "event_type": "purchase", "amount": 10},
        {"id": 2, "event_type": "page_view", "amount": None},
        {"id": 2, "event_type": "refund", "amount": -10},
        {"id": 3, "event_type": "oops", "amount": 0},
    ]

    r1 = check_unique(rows, "id")
    assert not r1.passed

    r2 = check_not_null(rows, "event_type")
    assert r2.passed

    r3 = check_not_null(rows, "amount")
    assert not r3.passed

    r4 = check_accepted_values(rows, "event_type", {"purchase", "refund", "page_view"})
    assert not r4.passed

    print("OK")
    for r in [r1, r2, r3, r4]:
        print(r.name, "PASSED" if r.passed else "FAILED", "examples:", r.failing_examples)


if __name__ == "__main__":
    run()
