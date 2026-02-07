"""
Dynamic Programming - Core Patterns (NeetCode 75 aligned)

Common patterns:
- 1D DP (House Robber, Climbing Stairs)
- 2D DP (Unique Paths)
- LIS / DP on sequences
- Coin Change

Time Complexity: depends on state transitions
Space Complexity: O(n) or O(n*m)
"""

from __future__ import annotations

from typing import List


def climbing_stairs(n: int) -> int:
    """Return number of ways to climb n stairs (1 or 2 steps)."""
    if n <= 2:
        return n
    prev2, prev1 = 1, 2
    for _ in range(3, n + 1):
        prev2, prev1 = prev1, prev1 + prev2
    return prev1


def house_robber(nums: List[int]) -> int:
    """Return max money without robbing adjacent houses."""
    rob1, rob2 = 0, 0
    for n in nums:
        rob1, rob2 = rob2, max(rob2, rob1 + n)
    return rob2


def coin_change(coins: List[int], amount: int) -> int:
    """Return min coins needed to make amount, or -1 if impossible."""
    dp = [amount + 1] * (amount + 1)
    dp[0] = 0
    for i in range(1, amount + 1):
        for c in coins:
            if i - c >= 0:
                dp[i] = min(dp[i], dp[i - c] + 1)
    return -1 if dp[amount] > amount else dp[amount]


def longest_increasing_subsequence(nums: List[int]) -> int:
    """Return length of LIS (O(n^2) DP)."""
    if not nums:
        return 0
    dp = [1] * len(nums)
    for i in range(len(nums)):
        for j in range(i):
            if nums[j] < nums[i]:
                dp[i] = max(dp[i], dp[j] + 1)
    return max(dp)


if __name__ == "__main__":
    print("=" * 70)
    print("Dynamic Programming - Examples")
    print("=" * 70)

    print("\n1. Climbing Stairs")
    print(climbing_stairs(5))

    print("\n2. House Robber")
    print(house_robber([2, 7, 9, 3, 1]))

    print("\n3. Coin Change")
    print(coin_change([1, 2, 5], 11))

    print("\n4. LIS")
    print(longest_increasing_subsequence([10, 9, 2, 5, 3, 7, 101, 18]))