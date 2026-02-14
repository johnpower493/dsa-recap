"""
Divide and Conquer - Practice Exercises

Divide and conquer is an algorithm design paradigm based on multi-branched recursion.
It works by recursively breaking down a problem into two or more sub-problems of the same type,
until these become simple enough to be solved directly.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Power Function
# Implement pow(x, n), which calculates x raised to the power n (i.e., x^n).
def my_pow(x, n):
    """Returns x raised to the power n.

    Examples:
        x=2.00000, n=10 -> 1024.00000
        x=2.10000, n=3 -> 9.26100
        x=2.00000, n=-2 -> 0.25000
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Sqrt(x)
# Given a non-negative integer x, return the square root of x rounded down to the nearest integer.
def my_sqrt(x):
    """Returns the square root of x floored to the nearest integer.

    Examples:
        x=4 -> 2
        x=8 -> 2
        x=0 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 3: Majority Element
# Given an array nums of size n, return the majority element.
# The majority element is the element that appears more than n/2 times.
# You may assume that the majority element always exists in the array.
def majority_element(nums):
    """Returns the majority element.

    Examples:
        [3,2,3] -> 3
        [2,2,1,1,1,2,2] -> 2
    """
    # YOUR SOLUTION HERE
    pass


# Problem 4: Search in Rotated Sorted Array
# There is an integer array nums sorted in ascending order (with distinct values).
# Prior to being passed to your function, nums is rotated at an unknown pivot index k.
# Given the array nums after the rotation and an integer target, return the index of target if it is in nums.
def search_rotated(nums, target):
    """Returns the index of target in rotated sorted array, or -1 if not found.

    Examples:
        nums=[4,5,6,7,0,1,2], target=0 -> 4
        nums=[4,5,6,7,0,1,2], target=3 -> -1
        nums=[1], target=0 -> -1
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Find Minimum in Rotated Sorted Array
# Find the minimum element in a rotated sorted array with unique elements.
def find_min(nums):
    """Returns the minimum element in rotated sorted array.

    Examples:
        [3,4,5,1,2] -> 1
        [4,5,6,7,0,1,2] -> 0
        [11,13,15,17] -> 11
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Merge Sort
# Implement the merge sort algorithm.
def merge_sort(arr):
    """Returns the sorted array using merge sort.

    Examples:
        [5,2,3,1] -> [1,2,3,5]
        [5,4,3,2,1] -> [1,2,3,4,5]
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 7: Median of Two Sorted Arrays
# Given two sorted arrays nums1 and nums2 of size m and n respectively,
# return the median of the two sorted arrays.
def find_median_sorted_arrays(nums1, nums2):
    """Returns the median of two sorted arrays.

    Examples:
        nums1=[1,3], nums2=[2] -> 2.0
        nums1=[1,2], nums2=[3,4] -> 2.5
        nums1=[], nums2=[1] -> 1.0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 8: Kth Largest Element in an Array
# Given an integer array nums and an integer k, return the kth largest element in the array.
# Note that it is the kth largest element in the sorted order, not the kth distinct element.
def find_kth_largest(nums, k):
    """Returns the kth largest element.

    Examples:
        nums=[3,2,1,5,6,4], k=2 -> 5
        nums=[3,2,3,1,2,4,5,5,6], k=4 -> 4
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


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 10: Count of Smaller Numbers After Self
# Given an integer array nums, return an integer array counts where counts[i]
# is the number of smaller elements to the right of nums[i].
def count_smaller(nums):
    """Returns array where each element is count of smaller numbers to its right.

    Examples:
        [5,2,6,1] -> [2,1,1,0]
        [-1,-1] -> [0,0]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 11: Inversion Count
# Given an array of integers, find the number of inversions in the array.
# Two elements a[i] and a[j] form an inversion if a[i] > a[j] and i < j.
def count_inversions(arr):
    """Returns the number of inversions in the array.

    Examples:
        [2,4,1,3,5] -> 3
        [5,4,3,2,1] -> 10
        [1,2,3,4,5] -> 0
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
def my_pow(x, n):
    """Fast exponentiation - O(log n) time, O(log n) space for recursion"""
    if n == 0:
        return 1.0
    
    if n < 0:
        return 1.0 / my_pow(x, -n)
    
    half = my_pow(x, n // 2)
    
    if n % 2 == 0:
        return half * half
    else:
        return half * half * x


def my_sqrt(x):
    """Binary search - O(log n) time, O(1) space"""
    if x < 2:
        return x
    
    left, right = 1, x // 2
    
    while left <= right:
        mid = left + (right - left) // 2
        squared = mid * mid
        
        if squared == x:
            return mid
        elif squared < x:
            left = mid + 1
        else:
            right = mid - 1
    
    return right


def majority_element(nums):
    """Divide and conquer - O(n log n) time, O(log n) space"""
    def majority_element_rec(left, right):
        # Base case: only one element
        if left == right:
            return nums[left]
        
        # Divide
        mid = left + (right - left) // 2
        left_majority = majority_element_rec(left, mid)
        right_majority = majority_element_rec(mid + 1, right)
        
        # If the two halves agree on the majority element, return it
        if left_majority == right_majority:
            return left_majority
        
        # Otherwise, count each element and return the winner
        left_count = sum(1 for i in range(left, right + 1) if nums[i] == left_majority)
        right_count = sum(1 for i in range(left, right + 1) if nums[i] == right_majority)
        
        return left_majority if left_count > right_count else right_majority
    
    return majority_element_rec(0, len(nums) - 1)


def search_rotated(nums, target):
    """Modified binary search - O(log n) time, O(1) space"""
    left, right = 0, len(nums) - 1
    
    while left <= right:
        mid = left + (right - left) // 2
        
        if nums[mid] == target:
            return mid
        
        # Check if left half is sorted
        if nums[left] <= nums[mid]:
            if nums[left] <= target < nums[mid]:
                right = mid - 1
            else:
                left = mid + 1
        # Right half is sorted
        else:
            if nums[mid] < target <= nums[right]:
                left = mid + 1
            else:
                right = mid - 1
    
    return -1


def find_min(nums):
    """Binary search - O(log n) time, O(1) space"""
    left, right = 0, len(nums) - 1
    
    while left < right:
        mid = left + (right - left) // 2
        
        if nums[mid] > nums[right]:
            # Minimum is in right half
            left = mid + 1
        else:
            # Minimum is in left half (including mid)
            right = mid
    
    return nums[left]


def merge_sort(arr):
    """Classic merge sort - O(n log n) time, O(n) space"""
    if len(arr) <= 1:
        return arr
    
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    
    return merge(left, right)


def merge(left, right):
    """Helper function to merge two sorted arrays"""
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


def find_median_sorted_arrays(nums1, nums2):
    """Binary search on smaller array - O(log(min(m,n))) time, O(1) space"""
    # Ensure nums1 is the smaller array
    if len(nums1) > len(nums2):
        nums1, nums2 = nums2, nums1
    
    m, n = len(nums1), len(nums2)
    left, right = 0, m
    
    while left <= right:
        partition1 = (left + right) // 2
        partition2 = (m + n + 1) // 2 - partition1
        
        max_left1 = float('-inf') if partition1 == 0 else nums1[partition1 - 1]
        min_right1 = float('inf') if partition1 == m else nums1[partition1]
        
        max_left2 = float('-inf') if partition2 == 0 else nums2[partition2 - 1]
        min_right2 = float('inf') if partition2 == n else nums2[partition2]
        
        if max_left1 <= min_right2 and max_left2 <= min_right1:
            # Found the correct partition
            if (m + n) % 2 == 0:
                return (max(max_left1, max_left2) + min(min_right1, min_right2)) / 2
            else:
                return max(max_left1, max_left2)
        elif max_left1 > min_right2:
            right = partition1 - 1
        else:
            left = partition1 + 1
    
    return 0.0


def find_kth_largest(nums, k):
    """Quickselect - O(n) average, O(n^2) worst, O(1) space"""
    def quickselect(left, right, k_smallest):
        if left == right:
            return nums[left]
        
        pivot_index = partition(left, right)
        
        if k_smallest == pivot_index:
            return nums[k_smallest]
        elif k_smallest < pivot_index:
            return quickselect(left, pivot_index - 1, k_smallest)
        else:
            return quickselect(pivot_index + 1, right, k_smallest)
    
    def partition(left, right):
        pivot_index = left
        pivot = nums[right]
        
        for i in range(left, right):
            if nums[i] < pivot:
                nums[i], nums[pivot_index] = nums[pivot_index], nums[i]
                pivot_index += 1
        
        nums[pivot_index], nums[right] = nums[right], nums[pivot_index]
        return pivot_index
    
    return quickselect(0, len(nums) - 1, len(nums) - k)


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
    
    # Convert to strings
    nums_str = list(map(str, nums))
    
    # Sort using custom comparator
    nums_str.sort(key=cmp_to_key(compare))
    
    # Handle case where result is "000..."
    if nums_str[0] == '0':
        return '0'
    
    return ''.join(nums_str)


def count_smaller(nums):
    """Merge sort with counting - O(n log n) time, O(n) space"""
    def merge_sort_count(enum):
        half = len(enum) // 2
        if half:
            left = merge_sort_count(enum[:half])
            right = merge_sort_count(enum[half:])
            
            i = j = 0
            while i < len(left) or j < len(right):
                if j == len(right) or (i < len(left) and left[i][1] <= right[j][1]):
                    enum[i + j] = left[i]
                    result[left[i][0]] += j
                    i += 1
                else:
                    enum[i + j] = right[j]
                    j += 1
        
        return enum
    
    result = [0] * len(nums)
    merge_sort_count(list(enumerate(nums)))
    return result


def count_inversions(arr):
    """Merge sort with counting - O(n log n) time, O(n) space"""
    def merge_sort_count(arr, temp, left, right):
        inv_count = 0
        if left < right:
            mid = (left + right) // 2
            inv_count += merge_sort_count(arr, temp, left, mid)
            inv_count += merge_sort_count(arr, temp, mid + 1, right)
            inv_count += merge(arr, temp, left, mid, right)
        return inv_count
    
    def merge(arr, temp, left, mid, right):
        i = left
        j = mid + 1
        k = left
        inv_count = 0
        
        while i <= mid and j <= right:
            if arr[i] <= arr[j]:
                temp[k] = arr[i]
                i += 1
            else:
                temp[k] = arr[j]
                inv_count += (mid - i + 1)
                j += 1
            k += 1
        
        while i <= mid:
            temp[k] = arr[i]
            i += 1
            k += 1
        
        while j <= right:
            temp[k] = arr[j]
            j += 1
            k += 1
        
        for i in range(left, right + 1):
            arr[i] = temp[i]
        
        return inv_count
    
    temp = [0] * len(arr)
    return merge_sort_count(arr.copy(), temp, 0, len(arr) - 1)
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Divide and Conquer Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)