# Two-Pointers Technique

A powerful algorithmic pattern that uses two pointers to traverse data structures efficiently.

## Quick Start

### Run the Examples
```bash
python two_pointers.py
```

### Practice Exercises
```bash
python two_pointers_exercise.py
```

## What is Two-Pointers?

The two-pointers technique uses two references (pointers) to traverse a data structure simultaneously. It's highly efficient, typically achieving **O(n) time complexity**.

## Three Main Patterns

### 1. Opposite Direction Pointers
Start at both ends and move toward each other.

**Use for:**
- Finding pairs that sum to a target (sorted arrays)
- Checking palindromes
- Container problems

**Example:**
```python
def two_sum_sorted(arr, target):
    left, right = 0, len(arr) - 1
    while left < right:
        current_sum = arr[left] + arr[right]
        if current_sum == target:
            return [left, right]
        elif current_sum < target:
            left += 1
        else:
            right -= 1
    return []
```

### 2. Same Direction Pointers (Fast & Slow)
Both start at the beginning, one moves faster.

**Use for:**
- Detecting cycles in linked lists
- Finding middle element
- Removing duplicates

**Example:**
```python
def remove_duplicates_sorted(arr):
    write_index = 1
    for i in range(1, len(arr)):
        if arr[i] != arr[write_index - 1]:
            arr[write_index] = arr[i]
            write_index += 1
    return write_index
```

### 3. Sliding Window
Two pointers define a window that slides across the array.

**Use for:**
- Subarray problems
- Longest/shortest substring problems
- Counting within ranges

**Example:**
```python
def longest_substring_without_repeating(s):
    char_set = set()
    left = 0
    max_length = 0
    
    for right in range(len(s)):
        while s[right] in char_set:
            char_set.remove(s[left])
            left += 1
        char_set.add(s[right])
        max_length = max(max_length, right - left + 1)
    
    return max_length
```

## Key Algorithms Implemented

| Algorithm | Pattern | Difficulty | Time | Space |
|-----------|---------|------------|------|-------|
| Two Sum (Sorted) | Opposite | Easy | O(n) | O(1) |
| Valid Palindrome | Opposite | Easy | O(n) | O(1) |
| Container With Most Water | Opposite | Medium | O(n) | O(1) |
| Remove Duplicates | Same Direction | Easy | O(n) | O(1) |
| Merge Sorted Arrays | Same Direction | Easy | O(n) | O(n) |
| Three Sum | Opposite + Sort | Medium | O(n²) | O(1) |
| Longest Substring | Sliding Window | Medium | O(n) | O(n) |
| Trapping Rain Water | Opposite | Hard | O(n) | O(1) |
| Sort Colors | Three Pointers | Medium | O(n) | O(1) |

## When to Use Two-Pointers

### Use When:
✅ Array is **sorted**
✅ Need to find **pairs** or **triplets**
✅ Need to **reverse** or **reorder**
✅ Looking for **subarrays** or **substrings**
✅ Checking for **symmetry** or **cycles**

### Don't Use When:
❌ Array is unsorted (sort first or use hash map)
❌ Need random access to non-adjacent elements
❌ Problem requires more than 2 pointers for tracking

## Common Pitfalls

1. **Off-by-one errors**: Be careful with pointer initialization and bounds
2. **Infinite loops**: Ensure pointers move toward each other
3. **Skipping duplicates**: Remember to skip duplicates in sorted arrays
4. **Not updating pointers**: Always update pointers after each comparison

## Practice Strategy

### Step 1: Understand the Pattern
Start with easy problems to grasp when to use each pattern.

### Step 2: Solve Medium Problems
Apply patterns to more complex scenarios.

### Step 3: Tackle Hard Problems
Combine patterns (e.g., sorting + two-pointers).

### Step 4: Optimize
Think about edge cases and optimization opportunities.

## Interview Tips

### Questions to Ask Yourself:
1. Is the array sorted? If yes, consider two-pointers.
2. Am I looking for pairs/triplets? Two-pointers is likely.
3. Can I solve this in one pass? Two-pointers often can.
4. Is there symmetry or reversal? Opposite pointers work well.

### Common Interview Questions:
- Two Sum (sorted array)
- Three Sum / Four Sum
- Container With Most Water
- Trapping Rain Water
- Longest Substring Without Repeating Characters
- Valid Palindrome
- Remove Duplicates from Sorted Array

## Complexity Cheat Sheet

| Problem | Time | Space |
|---------|------|-------|
| Two Sum (sorted) | O(n) | O(1) |
| Three Sum | O(n²) | O(1) |
| Four Sum | O(n³) | O(1) |
| Container Water | O(n) | O(1) |
| Trapping Rain | O(n) | O(1) |
| Longest Substring | O(n) | O(n) |
| Remove Duplicates | O(n) | O(1) |
| Merge Arrays | O(n) | O(n) |

## Resources

### LeetCode Tags:
- **Two Pointers**: 60+ problems
- **Sliding Window**: 30+ problems

### Related Concepts:
- Hash Maps (alternative for unsorted data)
- Binary Search (divide and conquer)
- Sorting (often combined with two-pointers)
- Greedy Algorithms (making optimal local choices)

## Next Steps

1. ✅ Run `two_pointers.py` to see all examples
2. ✅ Solve easy problems in `two_pointers_exercise.py`
3. ✅ Progress to medium problems
4. ✅ Challenge yourself with hard problems
5. ✅ Practice on LeetCode or similar platforms

## Key Takeaway

**Two-pointers = O(n) efficiency for many problems**

Mastering this technique will significantly improve your problem-solving speed and efficiency. Start with the pattern recognition, then practice until it becomes second nature!

---

**Remember**: The key is recognizing when to use which pattern. Practice makes perfect! 🚀
