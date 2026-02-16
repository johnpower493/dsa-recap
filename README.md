# DSA Recap

## SQL / Database Essentials
A new `sql/` folder contains Postgres-flavored worked examples for SQL and database essentials often used in data engineering (schema + seed data, joins, window functions, ETL/upserts, indexing, transactions, and data quality checks).

## Data Engineering Patterns
The `de_patterns/` folder contains exercises for common data engineering patterns using SQL + Python (incremental loads, dedup/idempotency, SCD Type 2, and data quality checks).

## Analytical Data Modelling
The `data_modelling/` folder contains Postgres SQL exercises for analytical modelling methodologies (star schema / dimensional modelling, snowflaking, factless facts + bridge tables, aggregate facts/rollups, and a conceptual Data Vault 2.0 mapping).

## Databricks (Data Engineering)
The `databricks/` folder contains Databricks-focused exercises (Spark DataFrames, Delta Lake, Medallion architecture, streaming, optimization, and job orchestration).

## Real-time Streaming
The `streaming/` folder covers real-time data pipeline patterns with Kafka, Kinesis, and stream processing (producers, consumers, windowing, exactly-once semantics, and backpressure handling).

## Infrastructure as Code (IaC)
The `iac/` folder covers automating data engineering infrastructure with Terraform, including Snowflake/Databricks resources, cloud infrastructure, secret management, and CI/CD for infrastructure.

## Advanced Data Quality
The `data_quality_advanced/` folder covers enterprise-grade data quality frameworks with Great Expectations and Soda Core, including automated monitoring, anomaly detection, and incident response frameworks.

## Production Deployment
The `production_deployment/` folder covers best practices for deploying data pipelines to production, including CI/CD strategies, blue-green deployments, monitoring, incident management, and security hardening.

## Modern Lakehouse Patterns
The `lakehouse/` folder covers modern data lakehouse technologies (Delta Lake, Apache Iceberg, Apache Hudi) including ACID transactions, schema evolution, time travel, and performance optimization.

## Specialized Topics
The `specialized_topics/` folder covers advanced specialized areas including MLOps, Data APIs, messaging systems, geospatial data engineering, data mesh, graph databases, and real-time analytics.


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

### 8. Heaps / Priority Queue

**Location:** `heaps/`

Heap-based patterns for top-k, k-th largest, and streaming median problems.

**Run examples:**
```bash
python heaps/heaps.py
```

**Practice Exercises:**
```bash
python heaps/heaps_exercise.py
```

### 9. Graphs

**Location:** `graphs/`

DFS/BFS traversals, course scheduling (topological sort), islands, and reachability.

**Run examples:**
```bash
python graphs/graphs.py
```

**Practice Exercises:**
```bash
python graphs/graphs_exercise.py
```

### 10. Backtracking

**Location:** `backtracking/`

Subsets, permutations, combination sum, and word search patterns.

**Run examples:**
```bash
python backtracking/backtracking.py
```

**Practice Exercises:**
```bash
python backtracking/backtracking_exercise.py
```

### 11. Dynamic Programming

**Location:** `dynamic_programming/`

Climbing stairs, house robber, coin change, LIS, and LCS patterns.

**Run examples:**
```bash
python dynamic_programming/dynamic_programming.py
```

**Practice Exercises:**
```bash
python dynamic_programming/dynamic_programming_exercise.py
```

### 12. Greedy Algorithms

**Location:** `greedy/`

Greedy algorithms make locally optimal choices at each step to find a global optimum.

**Key Implementations:**
- Maximum Subarray (Kadane's Algorithm)
- Jump Game / Jump Game II
- Gas Station
- Task Scheduler
- Partition Labels
- And more!

**Time Complexity:** O(n) to O(n log k)  
**Space Complexity:** O(1) to O(k)

**Practice Exercises:**
```bash
python greedy/greedy_exercise.py
```

### 13. Bit Manipulation

**Location:** `bit_manipulation/`

Bit manipulation techniques for low-level optimizations and specific algorithmic problems.

**Key Implementations:**
- Single Number (XOR operations)
- Number of 1 Bits (Hamming weight)
- Power of Two
- Reverse Bits
- Maximum XOR of Two Numbers
- And more!

**Time Complexity:** O(1) to O(n log k)  
**Space Complexity:** O(1) to O(k)

**Practice Exercises:**
```bash
python bit_manipulation/bit_manipulation_exercise.py
```

### 14. Divide and Conquer

**Location:** `divide_conquer/`

Divide and conquer recursively breaks problems into smaller subproblems.

**Key Implementations:**
- Power Function (fast exponentiation)
- Merge Sort
- Quickselect (Kth Largest)
- Search in Rotated Sorted Array
- Median of Two Sorted Arrays
- And more!

**Time Complexity:** O(log n) to O(n log n)  
**Space Complexity:** O(1) to O(n)

**Practice Exercises:**
```bash
python divide_conquer/divide_conquer_exercise.py
```

### 15. Sorting Algorithms

**Location:** `sorting/`

Classic and advanced sorting algorithms.

**Key Implementations:**
- Bubble Sort / Selection Sort / Insertion Sort
- Quick Sort / Heap Sort / Merge Sort
- Sort Colors (Dutch National Flag)
- Largest Number
- And more!

**Time Complexity:** O(n log n) for efficient sorts  
**Space Complexity:** O(1) to O(n)

**Practice Exercises:**
```bash
python sorting/sorting_exercise.py
```

### 16. Math Algorithms

**Location:** `math_algorithms/`

Mathematical algorithms and number theory problems.

**Key Implementations:**
- Plus One / Add Binary
- Factorial Trailing Zeroes
- Power / Sqrt / Divide
- Count Primes (Sieve of Eratosthenes)
- Roman to Integer / Integer to Roman
- And more!

**Time Complexity:** O(log n) to O(n log log n)  
**Space Complexity:** O(1) to O(n)

**Practice Exercises:**
```bash
python math_algorithms/math_algorithms_exercise.py
```

### 17. Union-Find (Disjoint Set Union)

**Location:** `union_find/`

Union-Find tracks elements partitioned into disjoint sets with efficient operations.

**Key Implementations:**
- Path Compression and Union by Rank
- Number of Connected Components
- Redundant Connection
- Accounts Merge
- Number of Islands II
- And more!

**Time Complexity:** O(α(n)) amortized (inverse Ackermann)  
**Space Complexity:** O(n)

**Practice Exercises:**
```bash
python union_find/union_find_exercise.py
```

### 18. Recursion

**Location:** `recursion/`

Recursion solves problems by breaking them into smaller instances of the same problem.

**Key Implementations:**
- Factorial / Fibonacci / Power
- Generate Parentheses
- Subsets / Permutations
- Combination Sum
- N-Queens
- And more!

**Time Complexity:** Varies (O(2^n) for naive, O(n) with memoization)  
**Space Complexity:** O(n) for call stack

**Practice Exercises:**
```bash
python recursion/recursion_exercise.py
```

### 19. Tries (Prefix Trees)

**Location:** `tries/`

Trie is a tree-like data structure for efficient string storage and prefix searches.

**Key Implementations:**
- Basic Trie (insert, search, starts_with)
- WordDictionary with wildcard support
- Trie with count tracking
- Autocomplete
- And more!

**Time Complexity:** O(L) where L is string length  
**Space Complexity:** O(N * L) where N is number of strings

**Run examples:**
```bash
python tries/tries.py
```

**Practice Exercises:**
```bash
python tries/tries_exercise.py
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

### Foundations
1. **Start with:** Binary Search (understand divide and conquer)
2. **Then:** Two-Pointers (build intuition for linear scans)
3. **Next:** Sliding Window (two-pointers + constraints)
4. **Then:** Hash Maps (counting, grouping, prefix sums)

### Data Structures
5. **Next:** Stacks (valid parentheses, monotonic stack)
6. **Then:** Linked Lists (pointer manipulation)
7. **Then:** Trees (DFS/BFS patterns)
8. **Then:** Heaps (top-k, streaming medians)
9. **Then:** Tries (prefix trees, autocomplete)

### Graphs & Advanced
10. **Then:** Graphs (BFS/DFS, topological sort)
11. **Then:** Union-Find (disjoint sets, connected components)

### Algorithm Design Paradigms
12. **Then:** Backtracking (combinatorial search)
13. **Then:** Recursion (base cases, recursive thinking)
14. **Then:** Dynamic Programming (state transition thinking)
15. **Then:** Greedy Algorithms (locally optimal choices)
16. **Then:** Divide and Conquer (recursive problem splitting)

### Specialized Topics
17. **Then:** Sorting Algorithms (comparison-based sorts)
18. **Then:** Bit Manipulation (XOR, bit shifting, bitwise ops)
19. **Then:** Math Algorithms (number theory, primes, GCD)

### Practice & Mastery
20. **Practice:** Exercises in each module
21. **Advanced:** Combine techniques (e.g., sliding window + hash map)

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

### Heaps Interview Problems
- Kth Largest Element
- Top K Frequent Elements
- K Closest Points
- Find Median from Data Stream

### Graphs Interview Problems
- Number of Islands
- Course Schedule
- Pacific Atlantic Water Flow
- Graph Valid Tree

### Backtracking Interview Problems
- Subsets / Subsets II
- Combination Sum / Combination Sum II
- Permutations
- Word Search

### Dynamic Programming Interview Problems
- Climbing Stairs
- House Robber
- Coin Change
- Longest Increasing Subsequence
- Longest Common Subsequence

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
| Heaps | O(n log k) | O(k) |
| Graphs | O(V + E) | O(V + E) |
| Backtracking | O(b^d) | O(d) + output |
| Dynamic Programming | O(n) to O(n*m) | O(n) to O(n*m) |

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
