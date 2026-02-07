"""
Heaps / Priority Queue - Core Patterns (NeetCode 75 aligned)

Heaps provide efficient access to min/max elements:
- Kth Largest Element in an Array (min-heap of size k)
- Top K Frequent Elements
- K Closest Points to Origin
- Last Stone Weight (max-heap)

Time Complexity: typically O(n log k) or O(n log n)
Space Complexity: O(k) or O(n)
"""

from __future__ import annotations

from collections import Counter
import heapq
from typing import List, Tuple


def kth_largest(nums: List[int], k: int) -> int:
    """Return the kth largest element in nums.

    Example:
        [3,2,1,5,6,4], k=2 -> 5
    """
    heap: List[int] = []
    for n in nums:
        heapq.heappush(heap, n)
        if len(heap) > k:
            heapq.heappop(heap)
    return heap[0]


def top_k_frequent(nums: List[int], k: int) -> List[int]:
    """Return the k most frequent elements (any order).

    Example:
        [1,1,1,2,2,3], k=2 -> [1,2]
    """
    counts = Counter(nums)
    heap: List[Tuple[int, int]] = [(-freq, val) for val, freq in counts.items()]
    heapq.heapify(heap)
    return [heapq.heappop(heap)[1] for _ in range(min(k, len(heap)))]


def k_closest(points: List[List[int]], k: int) -> List[List[int]]:
    """Return the k closest points to origin (0, 0).

    Example:
        [[1,3],[-2,2]], k=1 -> [[-2,2]]
    """
    heap: List[Tuple[int, List[int]]] = []
    for x, y in points:
        dist = x * x + y * y
        heapq.heappush(heap, (dist, [x, y]))
    return [heapq.heappop(heap)[1] for _ in range(min(k, len(heap)))]


def last_stone_weight(stones: List[int]) -> int:
    """Smash two heaviest stones until one remains.

    Example:
        [2,7,4,1,8,1] -> 1
    """
    max_heap = [-s for s in stones]
    heapq.heapify(max_heap)

    while len(max_heap) > 1:
        y = -heapq.heappop(max_heap)
        x = -heapq.heappop(max_heap)
        if y != x:
            heapq.heappush(max_heap, -(y - x))

    return -max_heap[0] if max_heap else 0


if __name__ == "__main__":
    print("=" * 70)
    print("Heaps - Examples")
    print("=" * 70)

    print("\n1. Kth Largest Element")
    print(kth_largest([3, 2, 1, 5, 6, 4], 2))

    print("\n2. Top K Frequent")
    print(top_k_frequent([1, 1, 1, 2, 2, 3], 2))

    print("\n3. K Closest Points")
    print(k_closest([[1, 3], [-2, 2], [2, -2]], 2))

    print("\n4. Last Stone Weight")
    print(last_stone_weight([2, 7, 4, 1, 8, 1]))