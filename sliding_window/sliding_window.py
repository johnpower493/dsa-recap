"""
Sliding Window Technique Implementation

The sliding window technique is an array/string processing pattern that maintains a window
[l, r] over the input while moving left/right pointers to satisfy a constraint.

Two common variants:
1) Fixed-size window: window size is constant (e.g., max sum of size k)
2) Variable-size window: expand right to include items, shrink left to restore constraint

Typical use cases:
- Longest/shortest subarray/substring satisfying a condition
- Counting subarrays/strings with constraints
- Running sums / averages

Time Complexity: usually O(n)
Space Complexity: often O(1) or O(k) / O(distinct chars)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple


def max_sum_subarray_k(nums: List[int], k: int) -> int:
    """Return the maximum sum of any contiguous subarray of size k.

    Args:
        nums: List of integers.
        k: Window size (must be >= 1).

    Returns:
        Maximum sum among all length-k windows.

    Raises:
        ValueError: if k is invalid.

    Example:
        nums=[2,1,5,1,3,2], k=3 -> 9 (5+1+3)
    """
    if k <= 0:
        raise ValueError("k must be >= 1")
    if k > len(nums):
        raise ValueError("k must be <= len(nums)")

    window_sum = sum(nums[:k])
    best = window_sum

    for r in range(k, len(nums)):
        window_sum += nums[r] - nums[r - k]
        best = max(best, window_sum)

    return best


def min_subarray_len(target: int, nums: List[int]) -> int:
    """Return the minimal length of a contiguous subarray with sum >= target.

    Variable-size window. Assumes nums contains non-negative integers.

    Args:
        target: Minimum required sum.
        nums: List of non-negative integers.

    Returns:
        Length of the smallest subarray with sum >= target, or 0 if none exists.

    Example:
        target=7, nums=[2,3,1,2,4,3] -> 2 ([4,3])
    """
    left = 0
    window_sum = 0
    best = float("inf")

    for right, x in enumerate(nums):
        window_sum += x

        while window_sum >= target:
            best = min(best, right - left + 1)
            window_sum -= nums[left]
            left += 1

    return 0 if best == float("inf") else int(best)


def longest_substring_k_distinct(s: str, k: int) -> int:
    """Return length of the longest substring with at most k distinct characters.

    Args:
        s: Input string.
        k: Maximum distinct characters allowed.

    Returns:
        Maximum length of a substring with <= k distinct characters.

    Example:
        s="eceba", k=2 -> 3 ("ece")
    """
    if k <= 0 or not s:
        return 0

    left = 0
    counts: Dict[str, int] = defaultdict(int)
    best = 0

    for right, ch in enumerate(s):
        counts[ch] += 1

        while len(counts) > k:
            left_ch = s[left]
            counts[left_ch] -= 1
            if counts[left_ch] == 0:
                del counts[left_ch]
            left += 1

        best = max(best, right - left + 1)

    return best


def longest_substring_no_repeat(s: str) -> int:
    """Return length of the longest substring without repeating characters.

    This is a sliding-window-with-last-seen-index variant.

    Args:
        s: Input string.

    Returns:
        Maximum length of a substring with all unique characters.

    Example:
        s="pwwkew" -> 3 ("wke")
    """
    last_seen: Dict[str, int] = {}
    left = 0
    best = 0

    for right, ch in enumerate(s):
        if ch in last_seen and last_seen[ch] >= left:
            left = last_seen[ch] + 1
        last_seen[ch] = right
        best = max(best, right - left + 1)

    return best


def find_anagram_starts(s: str, p: str) -> List[int]:
    """Find start indices of p's anagrams in s.

    Fixed-size window with frequency counts.

    Args:
        s: Text.
        p: Pattern.

    Returns:
        List of starting indices where an anagram of p begins.

    Example:
        s="cbaebabacd", p="abc" -> [0, 6]
    """
    if len(p) == 0 or len(p) > len(s):
        return []

    need: Dict[str, int] = defaultdict(int)
    for ch in p:
        need[ch] += 1

    have: Dict[str, int] = defaultdict(int)
    matches = 0
    required = len(need)

    res: List[int] = []
    left = 0

    for right, ch in enumerate(s):
        have[ch] += 1
        if ch in need and have[ch] == need[ch]:
            matches += 1

        # Keep window size == len(p)
        if right - left + 1 > len(p):
            left_ch = s[left]
            if left_ch in need and have[left_ch] == need[left_ch]:
                matches -= 1
            have[left_ch] -= 1
            if have[left_ch] == 0:
                del have[left_ch]
            left += 1

        if right - left + 1 == len(p) and matches == required:
            res.append(left)

    return res


def count_subarrays_sum_k(nums: List[int], k: int) -> int:
    """Count subarrays whose sum equals k.

    This is a classic prefix-sum + hash map pattern (often taught alongside sliding window).
    Works with negative numbers (unlike typical variable-size windows).

    Args:
        nums: List of integers.
        k: Target sum.

    Returns:
        Number of contiguous subarrays with sum exactly k.

    Example:
        nums=[1,1,1], k=2 -> 2
    """
    prefix_counts = defaultdict(int)
    prefix_counts[0] = 1

    total = 0
    prefix = 0

    for x in nums:
        prefix += x
        total += prefix_counts[prefix - k]
        prefix_counts[prefix] += 1

    return total


if __name__ == "__main__":
    print("=" * 70)
    print("Sliding Window Technique - Examples")
    print("=" * 70)

    # 1) Fixed-size: max sum of size k
    nums = [2, 1, 5, 1, 3, 2]
    k = 3
    print("\n1. Max sum subarray of size k")
    print(f"nums={nums}, k={k}")
    print(f"max_sum_subarray_k -> {max_sum_subarray_k(nums, k)}")

    # 2) Variable-size: min length with sum >= target
    nums = [2, 3, 1, 2, 4, 3]
    target = 7
    print("\n2. Min subarray length with sum >= target")
    print(f"nums={nums}, target={target}")
    print(f"min_subarray_len -> {min_subarray_len(target, nums)}")

    # 3) At most k distinct
    s = "eceba"
    k = 2
    print("\n3. Longest substring with at most k distinct")
    print(f"s='{s}', k={k}")
    print(f"longest_substring_k_distinct -> {longest_substring_k_distinct(s, k)}")

    # 4) No repeating characters
    s = "pwwkew"
    print("\n4. Longest substring without repeating characters")
    print(f"s='{s}'")
    print(f"longest_substring_no_repeat -> {longest_substring_no_repeat(s)}")

    # 5) Find anagram starts
    s = "cbaebabacd"
    p = "abc"
    print("\n5. Find anagram start indices")
    print(f"s='{s}', p='{p}'")
    print(f"find_anagram_starts -> {find_anagram_starts(s, p)}")

    # 6) Count subarrays with sum k
    nums = [1, 1, 1]
    k = 2
    print("\n6. Count subarrays sum == k")
    print(f"nums={nums}, k={k}")
    print(f"count_subarrays_sum_k -> {count_subarrays_sum_k(nums, k)}")
