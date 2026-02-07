"""
Heaps / Priority Queue - Practice Exercises (NeetCode 75 aligned)

Try solving the problems first. Solutions are included below (commented out).
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Last Stone Weight
# You are given an array of stones. Each turn, smash the two heaviest stones.

def last_stone_weight(stones):
    """Return the weight of the last remaining stone (or 0)."""
    # YOUR SOLUTION HERE
    pass


# Problem 2: Kth Largest Element in a Stream
# Design a class to add numbers and return the kth largest.

class KthLargest:
    """Store a running stream and return kth largest after each add."""

    def __init__(self, k, nums):
        # YOUR SOLUTION HERE
        pass

    def add(self, val):
        # YOUR SOLUTION HERE
        pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: K Closest Points to Origin

def k_closest(points, k):
    """Return k closest points to origin."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Top K Frequent Elements

def top_k_frequent(nums, k):
    """Return k most frequent elements."""
    # YOUR SOLUTION HERE
    pass


# Problem 5: Kth Largest Element in an Array

def find_kth_largest(nums, k):
    """Return kth largest element."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 6: Find Median from Data Stream

class MedianFinder:
    """Maintain a data stream and return median in O(log n)."""

    def __init__(self):
        # YOUR SOLUTION HERE
        pass

    def add_num(self, num):
        # YOUR SOLUTION HERE
        pass

    def find_median(self):
        # YOUR SOLUTION HERE
        pass


# =============================================================================
# SOLUTIONS (commented out)
# =============================================================================

print("=" * 80)
print("SOLUTIONS")
print("=" * 80)
print()
print("Try solving the problems above first!")
print("Uncomment the solutions section in the code to reveal the answers.")
print("=" * 80)
print()

"""
import heapq


def last_stone_weight(stones):
    max_heap = [-s for s in stones]
    heapq.heapify(max_heap)

    while len(max_heap) > 1:
        y = -heapq.heappop(max_heap)
        x = -heapq.heappop(max_heap)
        if y != x:
            heapq.heappush(max_heap, -(y - x))
    return -max_heap[0] if max_heap else 0


class KthLargest:
    def __init__(self, k, nums):
        self.k = k
        self.heap = nums[:]
        heapq.heapify(self.heap)
        while len(self.heap) > k:
            heapq.heappop(self.heap)

    def add(self, val):
        heapq.heappush(self.heap, val)
        if len(self.heap) > self.k:
            heapq.heappop(self.heap)
        return self.heap[0]


def k_closest(points, k):
    heap = []
    for x, y in points:
        dist = x * x + y * y
        heapq.heappush(heap, (dist, [x, y]))
    return [heapq.heappop(heap)[1] for _ in range(min(k, len(heap)))]


def top_k_frequent(nums, k):
    counts = {}
    for n in nums:
        counts[n] = counts.get(n, 0) + 1
    heap = [(-freq, val) for val, freq in counts.items()]
    heapq.heapify(heap)
    return [heapq.heappop(heap)[1] for _ in range(min(k, len(heap)))]


def find_kth_largest(nums, k):
    heap = []
    for n in nums:
        heapq.heappush(heap, n)
        if len(heap) > k:
            heapq.heappop(heap)
    return heap[0]


class MedianFinder:
    def __init__(self):
        self.small = []  # max-heap (negatives)
        self.large = []  # min-heap

    def add_num(self, num):
        if not self.large or num >= self.large[0]:
            heapq.heappush(self.large, num)
        else:
            heapq.heappush(self.small, -num)

        # Rebalance heaps
        if len(self.large) > len(self.small) + 1:
            heapq.heappush(self.small, -heapq.heappop(self.large))
        elif len(self.small) > len(self.large):
            heapq.heappush(self.large, -heapq.heappop(self.small))

    def find_median(self):
        if len(self.large) > len(self.small):
            return float(self.large[0])
        return (self.large[0] - self.small[0]) / 2.0
"""


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Heaps Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)