"""
Stacks - Core Patterns (NeetCode 75 aligned)

A stack is a LIFO (last-in first-out) structure.
Common patterns:
- Valid parentheses / bracket matching
- Min stack (support getMin in O(1))
- Evaluate Reverse Polish Notation
- Monotonic stack for "next greater" style problems (Daily Temperatures, Largest Rectangle, etc.)

Time Complexity: typically O(n)
Space Complexity: O(n)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


def is_valid_parentheses(s: str) -> bool:
    """Return True if parentheses/brackets are valid.

    Example:
        "()[]{}" -> True
        "(]" -> False
    """
    pairs = {')': '(', ']': '[', '}': '{'}
    stack: List[str] = []

    for ch in s:
        if ch in pairs:
            if not stack or stack[-1] != pairs[ch]:
                return False
            stack.pop()
        else:
            stack.append(ch)

    return not stack


class MinStack:
    """Stack supporting push/pop/top/get_min in O(1)."""

    def __init__(self) -> None:
        self._stack: List[int] = []
        self._mins: List[int] = []

    def push(self, val: int) -> None:
        self._stack.append(val)
        if not self._mins:
            self._mins.append(val)
        else:
            self._mins.append(min(val, self._mins[-1]))

    def pop(self) -> int:
        if not self._stack:
            raise IndexError("pop from empty stack")
        self._mins.pop()
        return self._stack.pop()

    def top(self) -> int:
        if not self._stack:
            raise IndexError("top from empty stack")
        return self._stack[-1]

    def get_min(self) -> int:
        if not self._mins:
            raise IndexError("min from empty stack")
        return self._mins[-1]


def eval_rpn(tokens: List[str]) -> int:
    """Evaluate Reverse Polish Notation.

    Supported operators: +, -, *, /
    Division truncates toward zero.

    Example:
        ["2","1","+","3","*"] -> 9
    """
    stack: List[int] = []
    ops = {"+", "-", "*", "/"}

    for t in tokens:
        if t not in ops:
            stack.append(int(t))
            continue

        b = stack.pop()
        a = stack.pop()

        if t == "+":
            stack.append(a + b)
        elif t == "-":
            stack.append(a - b)
        elif t == "*":
            stack.append(a * b)
        else:  # "/"
            stack.append(int(a / b))  # trunc toward 0

    return stack[-1] if stack else 0


def daily_temperatures(temperatures: List[int]) -> List[int]:
    """Monotonic decreasing stack to find next warmer day.

    Returns:
        res[i] = number of days until a warmer temperature; 0 if none.

    Example:
        [73,74,75,71,69,72,76,73] -> [1,1,4,2,1,1,0,0]
    """
    res = [0] * len(temperatures)
    stack: List[int] = []  # indices, temps decreasing

    for i, t in enumerate(temperatures):
        while stack and temperatures[stack[-1]] < t:
            j = stack.pop()
            res[j] = i - j
        stack.append(i)

    return res


def largest_rectangle_area(heights: List[int]) -> int:
    """Largest rectangle in histogram (monotonic increasing stack).

    Example:
        [2,1,5,6,2,3] -> 10
    """
    max_area = 0
    stack: List[tuple[int, int]] = []  # (start_index, height)

    for i, h in enumerate(heights):
        start = i
        while stack and stack[-1][1] > h:
            idx, height = stack.pop()
            max_area = max(max_area, height * (i - idx))
            start = idx
        stack.append((start, h))

    n = len(heights)
    for idx, height in stack:
        max_area = max(max_area, height * (n - idx))

    return max_area


if __name__ == "__main__":
    print("=" * 70)
    print("Stacks - Examples")
    print("=" * 70)

    # 1) Valid Parentheses
    print("\n1. Valid Parentheses")
    for s in ["()[]{}", "(]", "([{}])", "(((()" ]:
        print(f"{s!r} -> {is_valid_parentheses(s)}")

    # 2) MinStack
    print("\n2. MinStack")
    ms = MinStack()
    ms.push(-2)
    ms.push(0)
    ms.push(-3)
    print(f"get_min -> {ms.get_min()} (expect -3)")
    ms.pop()
    print(f"top -> {ms.top()} (expect 0)")
    print(f"get_min -> {ms.get_min()} (expect -2)")

    # 3) Eval RPN
    print("\n3. Evaluate RPN")
    tokens = ["2", "1", "+", "3", "*"]
    print(f"tokens={tokens} -> {eval_rpn(tokens)}")

    # 4) Daily Temperatures
    print("\n4. Daily Temperatures")
    temps = [73, 74, 75, 71, 69, 72, 76, 73]
    print(f"temps={temps}\n-> {daily_temperatures(temps)}")

    # 5) Largest Rectangle in Histogram
    print("\n5. Largest Rectangle in Histogram")
    heights = [2, 1, 5, 6, 2, 3]
    print(f"heights={heights} -> {largest_rectangle_area(heights)}")
