"""
Sliding Window Technique - Practice Exercises

Try to solve the problems on your own before looking at the solutions.

SOLUTIONS are included below each problem (commented out). Scroll down to see them.
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Maximum Average Subarray I
# Given an integer array and integer k, find the maximum average value of any contiguous subarray of length k.
# Return the maximum average.

def find_max_average(nums, k):
    """Returns max average among all length-k subarrays.

    Examples:
        nums=[1,12,-5,-6,50,3], k=4 -> 12.75
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Contains Duplicate II
# Given an integer array and an integer k, return True if there are two distinct indices i and j
# such that nums[i] == nums[j] and abs(i - j) <= k.

def contains_nearby_duplicate(nums, k):
    """Returns True if any value repeats within distance k.

    Examples:
        nums=[1,2,3,1], k=3 -> True
        nums=[1,0,1,1], k=1 -> True
        nums=[1,2,3,1,2,3], k=2 -> False
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: Longest Substring with At Most K Distinct Characters

def longest_substring_k_distinct(s, k):
    """Returns length of the longest substring with at most k distinct characters."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Minimum Size Subarray Sum
# Given an array of positive integers nums and a positive integer target,
# return the minimal length of a contiguous subarray of which the sum is >= target.
# If there is no such subarray, return 0.

def min_subarray_len(target, nums):
    """Returns minimal length of subarray with sum >= target."""
    # YOUR SOLUTION HERE
    pass


# Problem 5: Find All Anagrams in a String

def find_anagrams(s, p):
    """Returns list of starting indices of p's anagrams in s."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 6: Minimum Window Substring
# Given two strings s and t, return the minimum window in s which contains all the characters in t.
# If there is no such substring, return "".

def min_window(s, t):
    """Returns the minimum window substring containing all characters of t."""
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
from collections import defaultdict


def find_max_average(nums, k):
    if k <= 0:
        raise ValueError("k must be >= 1")
    if k > len(nums):
        raise ValueError("k must be <= len(nums)")

    window_sum = sum(nums[:k])
    best = window_sum

    for r in range(k, len(nums)):
        window_sum += nums[r] - nums[r - k]
        best = max(best, window_sum)

    return best / k


def contains_nearby_duplicate(nums, k):
    # Sliding window set of size at most k
    seen = set()
    left = 0

    for right, x in enumerate(nums):
        if x in seen:
            return True
        seen.add(x)

        if right - left >= k:
            seen.remove(nums[left])
            left += 1

    return False


def longest_substring_k_distinct(s, k):
    if k <= 0 or not s:
        return 0

    counts = defaultdict(int)
    left = 0
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


def min_subarray_len(target, nums):
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


def find_anagrams(s, p):
    if len(p) == 0 or len(p) > len(s):
        return []

    need = defaultdict(int)
    for ch in p:
        need[ch] += 1

    have = defaultdict(int)
    matches = 0
    required = len(need)

    left = 0
    res = []

    for right, ch in enumerate(s):
        have[ch] += 1
        if ch in need and have[ch] == need[ch]:
            matches += 1

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


def min_window(s, t):
    if not t:
        return ""

    need = defaultdict(int)
    for ch in t:
        need[ch] += 1

    window = defaultdict(int)
    have = 0
    required = len(need)

    best_len = float("inf")
    best_range = (-1, -1)

    left = 0
    for right, ch in enumerate(s):
        window[ch] += 1

        if ch in need and window[ch] == need[ch]:
            have += 1

        while have == required:
            if right - left + 1 < best_len:
                best_len = right - left + 1
                best_range = (left, right)

            left_ch = s[left]
            window[left_ch] -= 1
            if left_ch in need and window[left_ch] < need[left_ch]:
                have -= 1
            if window[left_ch] == 0:
                del window[left_ch]
            left += 1

    if best_len == float("inf"):
        return ""

    l, r = best_range
    return s[l : r + 1]
"""


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Sliding Window Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)
