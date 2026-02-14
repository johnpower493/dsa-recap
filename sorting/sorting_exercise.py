"""
Sorting Algorithms - Practice Exercises

Implement various sorting algorithms and understand their time and space complexities.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Bubble Sort
# Implement the bubble sort algorithm.
def bubble_sort(arr):
    """Returns sorted array using bubble sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [64,34,25,12,22,11,90] -> [11,12,22,25,34,64,90]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Selection Sort
# Implement the selection sort algorithm.
def selection_sort(arr):
    """Returns sorted array using selection sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [29,10,14,37,13] -> [10,13,14,29,37]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Insertion Sort
# Implement the insertion sort algorithm.
def insertion_sort(arr):
    """Returns sorted array using insertion sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [12,11,13,5,6] -> [5,6,11,12,13]
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Quick Sort
# Implement the quick sort algorithm with Lomuto partition scheme.
def quick_sort(arr):
    """Returns sorted array using quick sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [3,6,8,10,1,2,1] -> [1,1,2,3,6,8,10]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Heap Sort
# Implement the heap sort algorithm.
def heap_sort(arr):
    """Returns sorted array using heap sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [12,11,13,5,6,7] -> [5,6,7,11,12,13]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Sort Colors (Dutch National Flag Problem)
# Given an array nums with n objects colored red, white, or blue, sort them in-place.
# Use the integers 0, 1, and 2 to represent red, white, and blue.
def sort_colors(nums):
    """Sorts the array in-place with 0s, 1s, and 2s.

    Examples:
        [2,0,2,1,1,0] -> [0,0,1,1,2,2]
        [2,0,1] -> [0,1,2]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 7: Valid Anagram
# Given two strings s and t, return true if t is an anagram of s.
def is_anagram(s, t):
    """Returns True if t is an anagram of s.

    Examples:
        s="anagram", t="nagaram" -> True
        s="rat", t="car" -> False
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 8: Sort an Array
# Given an array of integers nums, sort the array in ascending order and return it.
# You must solve the problem without using any built-in functions.
def sort_array(nums):
    """Returns the sorted array without using built-in sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [5,1,1,2,0,0] -> [0,0,1,1,2,5]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 9: Largest Number
# Given a list of non-negative integers nums, arrange them such that they form the largest number.
def largest_number(nums):
    """Returns the largest number that can be formed.

    Examples:
        [10,2] -> "210"
        [3,30,34,5,9] -> "9534330"
    """
    # YOUR SOLUTION HERE
    pass


# Problem 10: Wiggle Sort
# Given an integer array nums, reorder it such that nums[0] <= nums[1] >= nums[2] <= nums[3]...
def wiggle_sort(nums):
    """Reorders array in wiggle pattern in-place.

    Examples:
        [3,5,2,1,6,4] -> [3,5,1,6,2,4] or any valid wiggle order
        [6,6,5,6,3,8] -> [6,6,5,6,3,8] or any valid wiggle order
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 11: Kth Smallest Element in a Sorted Matrix
# Given an n x n matrix where each row and column is sorted in ascending order,
# return the kth smallest element in the matrix.
def kth_smallest(matrix, k):
    """Returns the kth smallest element in sorted matrix.

    Examples:
        matrix=[[1,5,9],[10,11,13],[12,13,15]], k=8 -> 13
        matrix=[[-5]], k=1 -> -5
    """
    # YOUR SOLUTION HERE
    pass


# Problem 12: Count of Range Sum
# Given an integer array nums and two integers lower and upper, return the number of range sums
# that lie in [lower, upper] inclusive. Range sum S(i, j) is defined as the sum of elements
# in nums from indices i to j inclusive.
def count_range_sum(nums, lower, upper):
    """Returns the number of range sums in [lower, upper].

    Examples:
        nums=[-2,5,-1], lower=-2, upper=2 -> 3
        nums=[0], lower=0, upper=0 -> 1
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
def bubble_sort(arr):
    """Bubble Sort - O(n^2) time, O(1) space"""
    n = len(arr)
    arr = arr.copy()  # Don't modify original
    
    for i in range(n):
        swapped = False
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                swapped = True
        if not swapped:
            break
    
    return arr


def selection_sort(arr):
    """Selection Sort - O(n^2) time, O(1) space"""
    n = len(arr)
    arr = arr.copy()  # Don't modify original
    
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if arr[j] < arr[min_idx]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
    
    return arr


def insertion_sort(arr):
    """Insertion Sort - O(n^2) time, O(1) space"""
    arr = arr.copy()  # Don't modify original
    
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1
        
        while j >= 0 and arr[j] > key:
            arr[j + 1] = arr[j]
            j -= 1
        
        arr[j + 1] = key
    
    return arr


def quick_sort(arr):
    """Quick Sort - O(n log n) average, O(n^2) worst, O(log n) space"""
    if len(arr) <= 1:
        return arr
    
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    
    return quick_sort(left) + middle + quick_sort(right)


def heap_sort(arr):
    """Heap Sort - O(n log n) time, O(1) space"""
    arr = arr.copy()  # Don't modify original
    n = len(arr)
    
    def heapify(arr, n, i):
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2
        
        if left < n and arr[left] > arr[largest]:
            largest = left
        
        if right < n and arr[right] > arr[largest]:
            largest = right
        
        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            heapify(arr, n, largest)
    
    # Build max heap
    for i in range(n // 2 - 1, -1, -1):
        heapify(arr, n, i)
    
    # Extract elements from heap
    for i in range(n - 1, 0, -1):
        arr[0], arr[i] = arr[i], arr[0]
        heapify(arr, i, 0)
    
    return arr


def sort_colors(nums):
    """Dutch National Flag - O(n) time, O(1) space"""
    low, mid, high = 0, 0, len(nums) - 1
    
    while mid <= high:
        if nums[mid] == 0:
            nums[low], nums[mid] = nums[mid], nums[low]
            low += 1
            mid += 1
        elif nums[mid] == 1:
            mid += 1
        else:  # nums[mid] == 2
            nums[mid], nums[high] = nums[high], nums[mid]
            high -= 1


def is_anagram(s, t):
    """Character counting - O(n) time, O(1) space (fixed alphabet)"""
    if len(s) != len(t):
        return False
    
    count = [0] * 26
    for c in s:
        count[ord(c) - ord('a')] += 1
    for c in t:
        count[ord(c) - ord('a')] -= 1
        if count[ord(c) - ord('a')] < 0:
            return False
    
    return True


def sort_array(nums):
    """Using merge sort - O(n log n) time, O(n) space"""
    if len(nums) <= 1:
        return nums
    
    mid = len(nums) // 2
    left = sort_array(nums[:mid])
    right = sort_array(nums[mid:])
    
    return merge(left, right)


def merge(left, right):
    """Helper to merge two sorted arrays"""
    result = []
    i = j = 0
    
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    
    result.extend(left[i:])
    result.extend(right[j:])
    return result


def largest_number(nums):
    """Custom sorting - O(n log n) time, O(n) space"""
    from functools import cmp_to_key
    
    def compare(a, b):
        if a + b > b + a:
            return -1
        elif a + b < b + a:
            return 1
        else:
            return 0
    
    nums_str = list(map(str, nums))
    nums_str.sort(key=cmp_to_key(compare))
    
    if nums_str[0] == '0':
        return '0'
    
    return ''.join(nums_str)


def wiggle_sort(nums):
    """O(n) time, O(1) space - one pass swap"""
    for i in range(len(nums) - 1):
        if (i % 2 == 0) == (nums[i] > nums[i + 1]):
            nums[i], nums[i + 1] = nums[i + 1], nums[i]


def kth_smallest(matrix, k):
    """Binary search - O(n log(max-min)) time, O(1) space"""
    n = len(matrix)
    low, high = matrix[0][0], matrix[-1][-1]
    
    def count_less_equal(mid):
        count = 0
        row, col = n - 1, 0
        
        while row >= 0 and col < n:
            if matrix[row][col] <= mid:
                count += row + 1
                col += 1
            else:
                row -= 1
        
        return count
    
    while low < high:
        mid = (low + high) // 2
        count = count_less_equal(mid)
        
        if count < k:
            low = mid + 1
        else:
            high = mid
    
    return low


def count_range_sum(nums, lower, upper):
    """Merge sort with counting - O(n log n) time, O(n) space"""
    def count_and_sort(lo, hi):
        if lo == hi:
            return 0, [nums[lo]]
        
        mid = (lo + hi) // 2
        count_left, left = count_and_sort(lo, mid)
        count_right, right = count_and_sort(mid + 1, hi)
        
        count = count_left + count_right
        
        # Count range sums
        i = j = 0
        for prefix in left:
            while i < len(right) and right[i] - prefix < lower:
                i += 1
            while j < len(right) and right[j] - prefix <= upper:
                j += 1
            count += j - i
        
        # Merge sorted prefix sums
        merged = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] < right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                j += 1
        merged.extend(left[i:])
        merged.extend(right[j:])
        
        return count, merged
    
    # Calculate prefix sums
    prefix = [0]
    for num in nums:
        prefix.append(prefix[-1] + num)
    
    count, _ = count_and_sort(0, len(prefix) - 1)
    return count
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Sorting Algorithms Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)