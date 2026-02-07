"""
Backtracking - Core Patterns (NeetCode 75 aligned)

Common patterns:
- Subsets / subsets with duplicates
- Permutations
- Combination Sum
- Word Search

Time Complexity: typically exponential in n
Space Complexity: O(n) recursion depth + output size
"""

from __future__ import annotations

from typing import List


def subsets(nums: List[int]) -> List[List[int]]:
    """Return all subsets of nums.

    Example:
        [1,2] -> [[],[1],[2],[1,2]]
    """
    res: List[List[int]] = []

    def backtrack(i: int, path: List[int]) -> None:
        if i == len(nums):
            res.append(path[:])
            return
        # Exclude nums[i]
        backtrack(i + 1, path)
        # Include nums[i]
        path.append(nums[i])
        backtrack(i + 1, path)
        path.pop()

    backtrack(0, [])
    return res


def combination_sum(candidates: List[int], target: int) -> List[List[int]]:
    """Return combinations that sum to target (reuse allowed)."""
    res: List[List[int]] = []

    def backtrack(start: int, total: int, path: List[int]) -> None:
        if total == target:
            res.append(path[:])
            return
        if total > target:
            return
        for i in range(start, len(candidates)):
            path.append(candidates[i])
            backtrack(i, total + candidates[i], path)
            path.pop()

    backtrack(0, 0, [])
    return res


def permute(nums: List[int]) -> List[List[int]]:
    """Return all permutations of nums."""
    res: List[List[int]] = []
    used = [False] * len(nums)

    def backtrack(path: List[int]) -> None:
        if len(path) == len(nums):
            res.append(path[:])
            return
        for i, n in enumerate(nums):
            if used[i]:
                continue
            used[i] = True
            path.append(n)
            backtrack(path)
            path.pop()
            used[i] = False

    backtrack([])
    return res


def exist(board: List[List[str]], word: str) -> bool:
    """Return True if word exists in board via adjacent cells."""
    if not board or not word:
        return False

    rows, cols = len(board), len(board[0])

    def backtrack(r: int, c: int, idx: int) -> bool:
        if idx == len(word):
            return True
        if r < 0 or r >= rows or c < 0 or c >= cols:
            return False
        if board[r][c] != word[idx]:
            return False

        tmp = board[r][c]
        board[r][c] = "#"
        for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            if backtrack(r + dr, c + dc, idx + 1):
                board[r][c] = tmp
                return True
        board[r][c] = tmp
        return False

    for r in range(rows):
        for c in range(cols):
            if backtrack(r, c, 0):
                return True
    return False


if __name__ == "__main__":
    print("=" * 70)
    print("Backtracking - Examples")
    print("=" * 70)

    print("\n1. Subsets")
    print(subsets([1, 2, 3]))

    print("\n2. Combination Sum")
    print(combination_sum([2, 3, 6, 7], 7))

    print("\n3. Permutations")
    print(permute([1, 2, 3]))

    print("\n4. Word Search")
    board = [
        ["A", "B", "C", "E"],
        ["S", "F", "C", "S"],
        ["A", "D", "E", "E"],
    ]
    print(exist(board, "ABCCED"))