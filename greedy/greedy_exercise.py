"""
Greedy Algorithms - Practice Exercises

Greedy algorithms make locally optimal choices at each step with the hope of finding a global optimum.
They are often simpler and faster than other approaches but don't always work for every problem.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Maximum Subarray (Kadane's Algorithm)
# Find the contiguous subarray with the largest sum and return its sum.
def max_subarray(nums):
    """Returns the maximum sum of any contiguous subarray.

    Examples:
        [-2,1,-3,4,-1,2,1,-5,4] -> 6 (subarray [4,-1,2,1])
        [1] -> 1
        [5,4,-1,7,8] -> 23
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Jump Game
# Determine if you can reach the last index starting from index 0.
# Each element represents your maximum jump length at that position.
def can_jump(nums):
    """Returns True if you can reach the last index.

    Examples:
        [2,3,1,1,4] -> True
        [3,2,1,0,4] -> False
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 3: Jump Game II
# Find the minimum number of jumps to reach the last index.
def jump(nums):
    """Returns the minimum number of jumps to reach the last index.

    Examples:
        [2,3,1,1,4] -> 2
        [2,3,0,1,4] -> 2
    """
    # YOUR SOLUTION HERE
    pass


# Problem 4: Gas Station
# There are n gas stations along a circular route. Given two integer arrays gas and cost,
# return the starting gas station's index if you can travel around the circuit once in the clockwise direction.
def can_complete_circuit(gas, cost):
    """Returns the starting gas station index if possible, otherwise -1.

    Examples:
        gas=[1,2,3,4,5], cost=[3,4,5,1,2] -> 3
        gas=[2,3,4], cost=[3,4,3] -> -1
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Hand of Straights
# Given an array of integers hand where hand[i] is the value written on the ith card
# and an integer groupSize, return true if and only if she can rearrange the cards into groups.
def is_n_straight_hand(hand, groupSize):
    """Returns True if cards can be rearranged into groups of groupSize consecutive values.

    Examples:
        hand=[1,2,3,6,2,3,4,7,8], groupSize=3 -> True ([1,2,3],[2,3,4],[6,7,8])
        hand=[1,2,3,4,5], groupSize=4 -> False
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Partition Labels
# Partition the string into as many parts as possible so that each letter appears
# in at most one part. Return a list of integers representing the size of these parts.
def partition_labels(s):
    """Returns the sizes of each partition.

    Examples:
        "ababcbacadefegdehijhklij" -> [9,7,8]
        "eccbbbbdec" -> [10]
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 7: Task Scheduler
# Given a characters array tasks, representing the tasks a CPU needs to do,
# where each letter represents a different task. Tasks could be done in any order.
# Each task is done in one unit of time. For each unit of time, the CPU could complete
# either one task or just be idle. However, there is a non-negative integer n that
# represents the cooldown period between two same tasks.
def least_interval(tasks, n):
    """Returns the least number of units of times that the CPU will take to finish all tasks.

    Examples:
        tasks=["A","A","A","B","B","B"], n=2 -> 8
        tasks=["A","A","A","B","B","B"], n=0 -> 6
        tasks=["A","A","A","A","A","A","B","C","D","E","F","G"], n=2 -> 16
    """
    # YOUR SOLUTION HERE
    pass


# Problem 8: Reorganize String
# Given a string s, rearrange the characters of s so that any two adjacent characters are not the same.
# Return any possible rearrangement of s or return "" if not possible.
def reorganize_string(s):
    """Returns a rearranged string with no adjacent same characters, or "" if impossible.

    Examples:
        "aab" -> "aba"
        "aaab" -> ""
    """
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

'''
from collections import Counter
import heapq


def max_subarray(nums):
    """Kadane's Algorithm - O(n) time, O(1) space"""
    if not nums:
        return 0
    
    max_current = max_global = nums[0]
    
    for num in nums[1:]:
        max_current = max(num, max_current + num)
        max_global = max(max_global, max_current)
    
    return max_global


def can_jump(nums):
    """Greedy - O(n) time, O(1) space"""
    max_reach = 0
    
    for i, num in enumerate(nums):
        if i > max_reach:
            return False
        max_reach = max(max_reach, i + num)
    
    return True


def jump(nums):
    """Greedy BFS-like approach - O(n) time, O(1) space"""
    if len(nums) <= 1:
        return 0
    
    jumps = 0
    current_end = 0
    farthest = 0
    
    for i in range(len(nums) - 1):
        farthest = max(farthest, i + nums[i])
        
        if i == current_end:
            jumps += 1
            current_end = farthest
            
            if current_end >= len(nums) - 1:
                break
    
    return jumps


def can_complete_circuit(gas, cost):
    """Greedy - O(n) time, O(1) space"""
    total_gas = 0
    total_cost = 0
    current_gas = 0
    start = 0
    
    for i in range(len(gas)):
        total_gas += gas[i]
        total_cost += cost[i]
        current_gas += gas[i] - cost[i]
        
        if current_gas < 0:
            start = i + 1
            current_gas = 0
    
    return start if total_gas >= total_cost else -1


def is_n_straight_hand(hand, groupSize):
    """Greedy with Counter - O(n log n) time, O(n) space"""
    if len(hand) % groupSize != 0:
        return False
    
    count = Counter(hand)
    
    while count:
        # Get the smallest card
        start = min(count.keys())
        
        # Try to form a group starting from this card
        for i in range(start, start + groupSize):
            if count[i] == 0:
                return False
            count[i] -= 1
            if count[i] == 0:
                del count[i]
    
    return True


def partition_labels(s):
    """Greedy - O(n) time, O(1) space (since alphabet is fixed)"""
    # Find the last occurrence of each character
    last_occurrence = {char: idx for idx, char in enumerate(s)}
    
    partitions = []
    start = end = 0
    
    for idx, char in enumerate(s):
        end = max(end, last_occurrence[char])
        
        if idx == end:
            partitions.append(end - start + 1)
            start = idx + 1
    
    return partitions


def least_interval(tasks, n):
    """Greedy with max-heap - O(n log k) time, O(k) space"""
    if n == 0:
        return len(tasks)
    
    # Count task frequencies
    task_count = Counter(tasks)
    max_heap = [-count for count in task_count.values()]
    heapq.heapify(max_heap)
    
    time = 0
    
    while max_heap or task_count:
        if not max_heap:
            # Idle time needed
            time = max(time, max(task_count.values()) * (n + 1) - n)
            break
        
        # Process most frequent tasks
        current_tasks = []
        for _ in range(min(n + 1, len(max_heap))):
            if max_heap:
                current_tasks.append(-heapq.heappop(max_heap))
        
        time += len(current_tasks)
        
        # Add tasks back to heap if they still have counts remaining
        for task in current_tasks:
            if task - 1 > 0:
                heapq.heappush(max_heap, -(task - 1))
    
    return time


def reorganize_string(s):
    """Greedy with max-heap - O(n log k) time, O(k) space"""
    if not s:
        return ""
    
    # Count character frequencies
    char_count = Counter(s)
    
    # Check if rearrangement is possible
    max_freq = max(char_count.values())
    if max_freq > (len(s) + 1) // 2:
        return ""
    
    # Build max-heap (negate for max-heap behavior)
    max_heap = [(-count, char) for char, count in char_count.items()]
    heapq.heapify(max_heap)
    
    result = []
    
    while len(max_heap) > 1:
        # Get two most frequent characters
        count1, char1 = heapq.heappop(max_heap)
        count2, char2 = heapq.heappop(max_heap)
        
        result.extend([char1, char2])
        
        # Add them back to heap if they still have counts remaining
        if count1 + 1 < 0:
            heapq.heappush(max_heap, (count1 + 1, char1))
        if count2 + 1 < 0:
            heapq.heappush(max_heap, (count2 + 1, char2))
    
    # Handle the last character if any
    if max_heap:
        count, char = heapq.heappop(max_heap)
        if count != -1:
            return ""  # Should not happen if we checked properly
        result.append(char)
    
    return "".join(result)
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Greedy Algorithms Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)