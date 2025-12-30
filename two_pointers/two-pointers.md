## __What is the Two-Pointers Technique?__

The two-pointers technique uses two pointers to traverse a data structure (usually an array or string) simultaneously. It's highly efficient because it processes elements in a single pass, achieving O(n) time complexity.

## __Common Two-Pointer Patterns__

### 1. __Opposite Direction Pointers__

- One pointer starts at the beginning, one at the end
- Move towards each other
- Used for: palindrome checking, two-sum in sorted array, container problems

### 2. __Same Direction Pointers (Fast & Slow)__

- Both pointers start at the beginning
- One moves faster than the other
- Used for: cycle detection, finding middle element, removing duplicates

### 3. __Sliding Window (Fixed/Variable)__

- Two pointers define a window
- Slide the window across the array
- Used for: subarray problems, longest/shortest substrings

## __Classic Problems Solved by Two-Pointers__

1. __Two Sum (Sorted Array)__ - Find two numbers that add up to target
2. __Valid Palindrome__ - Check if string reads same forwards/backwards
3. __Container With Most Water__ - Maximize area between two lines
4. __Remove Duplicates from Sorted Array__ - In-place deduplication
5. __Three Sum__ - Find triplets that sum to zero
6. __Merge Two Sorted Arrays__ - Combine sorted lists
7. __Longest Substring Without Repeating Characters__
8. __Reversing Linked List__ - Using prev and current pointers
