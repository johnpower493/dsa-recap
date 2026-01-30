"""
Hash Maps (Dictionaries) - Core Patterns

Hash maps (Python dict) provide average O(1) insertion/lookup.
They are a foundational tool for:
- Frequency counting
- Grouping / bucketing
- Deduplication and membership tests
- Prefix sum counting
- Sliding window bookkeeping

This module provides common patterns with clear implementations and examples.
"""

from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict, Dict, List, Tuple


def two_sum(nums: List[int], target: int) -> List[int]:
    """Return indices of two numbers that add up to target.

    Uses a hash map from value -> index.

    Args:
        nums: List of integers.
        target: Target sum.

    Returns:
        A list [i, j] with i < j if a solution exists, else [].

    Example:
        nums=[2,7,11,15], target=9 -> [0,1]
    """
    seen: Dict[int, int] = {}

    for i, x in enumerate(nums):
        want = target - x
        if want in seen:
            return [seen[want], i]
        seen[x] = i

    return []


def frequency_count(items: List[str]) -> Dict[str, int]:
    """Count occurrences of each item."""
    counts: DefaultDict[str, int] = defaultdict(int)
    for it in items:
        counts[it] += 1
    return dict(counts)


def group_anagrams(words: List[str]) -> List[List[str]]:
    """Group anagrams together.

    Key idea: anagrams share the same sorted-character signature.

    Time: O(n * m log m) (m = average word length)

    Returns:
        List of groups (order not guaranteed).

    Example:
        ["eat","tea","tan","ate","nat","bat"] -> [["eat","tea","ate"],["tan","nat"],["bat"]]
    """
    groups: DefaultDict[Tuple[str, ...], List[str]] = defaultdict(list)

    for w in words:
        signature = tuple(sorted(w))
        groups[signature].append(w)

    return list(groups.values())


def first_unique_char(s: str) -> int:
    """Return index of first non-repeating character, or -1 if none exists."""
    counts: DefaultDict[str, int] = defaultdict(int)
    for ch in s:
        counts[ch] += 1

    for i, ch in enumerate(s):
        if counts[ch] == 1:
            return i

    return -1


def longest_consecutive(nums: List[int]) -> int:
    """Return length of the longest consecutive sequence (LeetCode 128).

    Uses a set (hash-based) to expand only from starts of sequences.

    Time: O(n) average.
    """
    if not nums:
        return 0

    s = set(nums)
    best = 0

    for x in s:
        if x - 1 not in s:  # start of a sequence
            y = x
            while y in s:
                y += 1
            best = max(best, y - x)

    return best


def subarray_sum_equals_k(nums: List[int], k: int) -> int:
    """Count number of subarrays whose sum equals k.

    Prefix sum + hashmap counts.

    Works with negative numbers.

    Example:
        nums=[1,1,1], k=2 -> 2
    """
    prefix_counts: DefaultDict[int, int] = defaultdict(int)
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
    print("Hash Maps (dict) - Examples")
    print("=" * 70)

    # 1) Two Sum
    nums = [2, 7, 11, 15]
    target = 9
    print("\n1. Two Sum")
    print(f"nums={nums}, target={target}")
    print(f"two_sum -> {two_sum(nums, target)}")

    # 2) Frequency counting
    items = ["apple", "banana", "apple", "orange", "banana", "apple"]
    print("\n2. Frequency Count")
    print(f"items={items}")
    print(f"frequency_count -> {frequency_count(items)}")

    # 3) Group anagrams
    words = ["eat", "tea", "tan", "ate", "nat", "bat"]
    print("\n3. Group Anagrams")
    print(f"words={words}")
    print(f"group_anagrams -> {group_anagrams(words)}")

    # 4) First unique character
    s = "leetcode"
    print("\n4. First Unique Character")
    print(f"s='{s}'")
    print(f"first_unique_char -> {first_unique_char(s)}")

    # 5) Longest consecutive sequence
    nums = [100, 4, 200, 1, 3, 2]
    print("\n5. Longest Consecutive Sequence")
    print(f"nums={nums}")
    print(f"longest_consecutive -> {longest_consecutive(nums)}")

    # 6) Subarray sum equals k
    nums = [1, 1, 1]
    k = 2
    print("\n6. Subarray Sum Equals K")
    print(f"nums={nums}, k={k}")
    print(f"subarray_sum_equals_k -> {subarray_sum_equals_k(nums, k)}")
