# DSA Recap

## SQL / Database Essentials
A new `sql/` folder contains Postgres-flavored worked examples for SQL and database essentials often used in data engineering (schema + seed data, joins, window functions, ETL/upserts, indexing, transactions, and data quality checks).

## Data Engineering Patterns
The `de_patterns/` folder contains exercises for common data engineering patterns using SQL + Python (incremental loads, dedup/idempotency, SCD Type 2, and data quality checks).


A comprehensive collection of Data Structures and Algorithms implementations with detailed explanations, examples, and practice exercises.

## Overview

This repository contains implementations of essential DSA concepts with clear documentation, time/space complexity analysis, and practical examples. Each module includes:

- **Core implementations** with detailed comments
- **Time and space complexity** analysis
- **Comprehensive test cases** covering edge cases
- **Practice exercises** with solutions

## Modules

### 1. Binary Search

**Location:** `binary_search/`

Binary search is an efficient algorithm for finding an item from a sorted list by repeatedly dividing the search interval in half.

**Key Implementations:**
- Iterative binary search
- Recursive binary search
- Finding first/last occurrence in sorted array with duplicates
- Finding insert position in sorted array

**Time Complexity:** O(log n)  
**Space Complexity:** O(1) iterative, O(log n) recursive

**Usage:**
```python
from binary_search.binary_search import binary_search, find_first_occurrence

# Basic search
arr = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
result = binary_search(arr, 23)  # Returns index 5

# Find first occurrence
arr_with_dupes = [1, 2, 2, 2, 3, 4, 4, 5, 6]
first = find_first_occurrence(arr_with_dupes, 2)  # Returns index 1
```

**Run examples:**
```bash
python binary_search/binary_search.py
```

### 2. Two-Pointers Technique

**Location:** `two_pointers/`

The two-pointers technique uses two pointers to traverse a data structure simultaneously, achieving O(n) time complexity for many problems.

**Key Patterns:**
- **Opposite Direction:** Pointers start at both ends and move toward each other
- **Same Direction (Fast & Slow):** Both start at beginning, one moves faster
- **Sliding Window:** Pointers define a window that slides across the array

**Key Implementations:**
- Two Sum in Sorted Array
- Valid Palindrome
- Container With Most Water
- Remove Duplicates from Sorted Array
- Merge Sorted Arrays
- Three Sum
- Longest Substring Without Repeating Characters
- Trapping Rain Water
- And more!

**Time Complexity:** O(n) for most implementations  
**Space Complexity:** O(1) for in-place operations, O(n) if creating new arrays

**Usage:**
```python
from two_pointers.two_pointers import (
    two_sum_sorted,
    is_palindrome,
    max_area,
    remove_duplicates_sorted
)

# Two sum
arr = [2, 7, 11, 15, 17, 19]
result = two_sum_sorted(arr, 26)  # Returns [0, 5]

# Palindrome check
is_pal = is_palindrome("racecar")  # Returns True

# Max water container
water = max_area([1, 8, 6, 2, 5, 4, 8, 3, 7])  # Returns 49
```

**Run examples:**
```bash
python two_pointers/two_pointers.py
```

**Practice Exercises:**
```bash
python two_pointers/two_pointers_exercise.py
```

### 3. Sliding Window Technique

**Location:** `sliding_window/`

The sliding window technique maintains a moving window over an array/string to satisfy constraints efficiently.

**Key Patterns:**
- **Fixed-size window:** e.g., max sum/average of length k
- **Variable-size window:** e.g., smallest window with sum >= target
- **Frequency window:** e.g., anagrams, min window substring

**Run examples:**
```bash
python sliding_window/sliding_window.py
```

**Practice Exercises:**
```bash
python sliding_window/sliding_window_exercise.py
```

### 4. Hash Maps (Dictionaries)

**Location:** `hash_maps/`

Hash maps (Python `dict`) provide average O(1) lookup/insert and power many patterns: counting, grouping, and prefix sums.

**Key Implementations:**
- Two Sum (unsorted)
- Frequency counting
- Group Anagrams
- Longest Consecutive Sequence
- Subarray Sum Equals K (prefix sums)

**Run examples:**
```bash
python hash_maps/hash_maps.py
```

**Practice Exercises:**
```bash
python hash_maps/hash_maps_exercise.py
```

### 5. Stacks (incl. Monotonic Stack)

**Location:** `stacks/`

Stacks power bracket validation, min-stack design, expression evaluation, and monotonic stack problems.

**Run examples:**
```bash
python stacks/stacks.py
```

**Practice Exercises:**
```bash
python stacks/stacks_exercise.py
```

### 6. Linked Lists

**Location:** `linked_lists/`

Classic in-place pointer manipulation problems.

**Run examples:**
```bash
python linked_lists/linked_lists.py
```

**Practice Exercises:**
```bash
python linked_lists/linked_lists_exercise.py
```

### 7. Trees (Binary Trees / BST)

**Location:** `trees/`

Tree traversals, BFS/DFS patterns, depth, and BST validation.

**Run examples:**
```bash
python trees/trees.py
```

**Practice Exercises:**
```bash
python trees/trees_exercise.py
```

## Practice Exercises

Each module includes an exercise file with problems ranging from Easy to Hard difficulty:

- **Easy:** Perfect for beginners to grasp the concept
- **Medium:** Intermediate problems requiring deeper understanding
- **Hard:** Advanced problems combining multiple concepts
- **Bonus:** Challenging problems for mastery

Solutions are included but commented out - try solving problems on your own first!

## Learning Path

Recommended order to study these modules:

1. **Start with:** Binary Search (understand divide and conquer)
2. **Then:** Two-Pointers (build intuition for linear scans)
3. **Next:** Sliding Window (two-pointers + constraints)
4. **Then:** Hash Maps (counting, grouping, prefix sums)
5. **Next:** Stacks (valid parentheses, monotonic stack)
6. **Then:** Linked Lists (pointer manipulation)
7. **Then:** Trees (DFS/BFS patterns)
8. **Practice:** Exercises in each module
9. **Advanced:** Combine techniques (e.g., sliding window + hash map)

## Common Interview Topics

These modules cover frequently asked interview topics:

### Binary Search Interview Problems
- Search in Rotated Sorted Array
- Find Minimum in Rotated Sorted Array
- Search Insert Position
- Find Peak Element

### Two-Pointers Interview Problems
- Two Sum (sorted array)
- Three Sum / Four Sum
- Trapping Rain Water
- Container With Most Water
- Longest Substring Without Repeating Characters
- Valid Palindrome variants

## Key Concepts to Master

### Binary Search
- ✅ Divide and conquer approach
- ✅ Handling edge cases (empty array, single element)
- ✅ Avoiding integer overflow in middle calculation
- ✅ Finding first/last occurrence with duplicates

### Two-Pointers
- ✅ Recognizing when to use opposite vs same direction
- ✅ Fast and slow pointer for linked lists
- ✅ Sliding window for subarray/string problems
- ✅ In-place array manipulation

## Complexity Analysis

| Module | Typical Time | Typical Space |
|--------|-------------|---------------|
| Binary Search | O(log n) | O(1) |
| Two-Pointers | O(n) | O(1) |
| Sliding Window | O(n) | O(1) to O(distinct) |
| Hash Maps | O(n) | O(n) |
| Stacks | O(n) | O(n) |
| Linked Lists | O(n) | O(1) |
| Trees | O(n) | O(h) to O(n) |

## Testing

All implementations include comprehensive test cases:

- ✅ Basic functionality tests
- ✅ Edge cases (empty arrays, single elements)
- ✅ Boundary conditions
- ✅ Duplicates handling
- ✅ Large input scenarios

## Contributing

Feel free to add more DSA implementations, improve existing code, or add new practice problems!

## License

This repository is for educational purposes.

---

**Happy Learning! 🚀**

Remember: The best way to learn DSA is through practice. Try solving the exercises before looking at solutions!
