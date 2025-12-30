"""
Two-Pointers Technique - Practice Exercises

This file contains practice problems ranging from easy to hard.
Try to solve them on your own before looking at the solutions!

SOLUTIONS are included below each problem. Scroll down to see them.
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Valid Palindrome II
# Given a string, determine if it can be made a palindrome by deleting at most one character.
def valid_palindrome_ii(s):
    """
    Returns True if s can be made a palindrome by deleting at most one character.
    
    Examples:
        "aba" -> True (already a palindrome)
        "abca" -> True (delete 'b' or 'c')
        "abc" -> False
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Squares of a Sorted Array
# Given a sorted array of integers (may contain negatives), return squares of each number in sorted order.
def sorted_squares(nums):
    """
    Returns sorted array of squares.
    
    Examples:
        [-4, -1, 0, 3, 10] -> [0, 1, 9, 16, 100]
        [-7, -3, 2, 3, 11] -> [4, 9, 9, 49, 121]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Move Zeroes
# Move all zeroes to the end while maintaining relative order of non-zero elements.
def move_zeroes(arr):
    """
    Moves all zeroes to end in-place.
    
    Examples:
        [0, 1, 0, 3, 12] -> [1, 3, 12, 0, 0]
        [0, 0, 1] -> [1, 0, 0]
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Two Sum II - Input Array is Sorted
# Similar to two_sum_sorted, but return 1-based indices.
def two_sum_ii(arr, target):
    """
    Returns 1-based indices of two numbers that sum to target.
    
    Examples:
        [2, 7, 11, 15], target=9 -> [1, 2]
        [2, 3, 4], target=6 -> [1, 3]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: 3Sum Closest
# Find the triplet whose sum is closest to a target value.
def three_sum_closest(nums, target):
    """
    Returns the sum of triplet closest to target.
    
    Examples:
        [-1, 2, 1, -4], target=1 -> 2 (-1 + 2 + 1)
        [0, 0, 0], target=1 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Four Sum
# Find all unique quadruplets that sum to target.
def four_sum(nums, target):
    """
    Returns all unique quadruplets that sum to target.
    
    Examples:
        [1, 0, -1, 0, -2, 2], target=0 -> [[-2,-1,1,2],[-2,0,0,2],[-1,0,0,1]]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 7: Reverse Words in a String
# Reverse the order of words in a string (in-place if possible).
def reverse_words(s):
    """
    Returns string with words reversed.
    
    Examples:
        "the sky is blue" -> "blue is sky the"
        "  hello world  " -> "world hello"
    """
    # YOUR SOLUTION HERE
    pass


# Problem 8: Container With Most Water - Variation
# Find the two lines that together form the container with maximum area.
# Return the indices (0-based) of these lines.
def max_water_indices(heights):
    """
    Returns the indices of lines forming max water container.
    
    Examples:
        [1,8,6,2,5,4,8,3,7] -> [1, 8] (lines at indices 1 and 8)
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 9: Trapping Rain Water
# Given n non-negative integers representing height bars, compute how much water it can trap.
def trap(height):
    """
    Returns total amount of trapped water.
    
    Examples:
        [0,1,0,2,1,0,1,3,2,1,2,1] -> 6
        [4,2,0,3,2,5] -> 9
    """
    # YOUR SOLUTION HERE
    pass


# Problem 10: Find Minimum in Rotated Sorted Array
# Find the minimum element in a rotated sorted array with unique elements.
def find_min_rotated(nums):
    """
    Returns the minimum element.
    
    Examples:
        [3,4,5,1,2] -> 1
        [4,5,6,7,0,1,2] -> 0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 11: Longest Substring with At Most K Distinct Characters
# Find the length of the longest substring with at most K distinct characters.
def length_of_longest_substring_k_distinct(s, k):
    """
    Returns the maximum length of substring with at most K distinct characters.
    
    Examples:
        "eceba", k=2 -> 3 ("ece")
        "aa", k=1 -> 2
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS CHALLENGE
# =============================================================================

# Problem 12: Subarray Sum Equals K
# Find total number of subarrays that sum to K.
def subarray_sum(nums, k):
    """
    Returns the count of subarrays that sum to K.
    
    Examples:
        [1,1,1], k=2 -> 2
        [1,2,3], k=3 -> 2
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# SOLUTIONS (Scroll down to see answers)
# =============================================================================

print("=" * 80)
print("SOLUTIONS")
print("=" * 80)
print()
print("Try solving the problems above first!")
print("Type 'solutions' in the input to reveal the answers.")
print("=" * 80)
print()

# Uncomment below to see solutions
"""
def valid_palindrome_ii(s):
    def is_palindrome(left, right):
        while left < right:
            if s[left] != s[right]:
                return False
            left += 1
            right -= 1
        return True
    
    left, right = 0, len(s) - 1
    while left < right:
        if s[left] != s[right]:
            # Try deleting either left or right character
            return is_palindrome(left + 1, right) or is_palindrome(left, right - 1)
        left += 1
        right -= 1
    return True


def sorted_squares(nums):
    n = len(nums)
    result = [0] * n
    left, right = 0, n - 1
    pos = n - 1  # Fill from right to left (largest to smallest)
    
    while left <= right:
        left_sq = nums[left] ** 2
        right_sq = nums[right] ** 2
        
        if left_sq > right_sq:
            result[pos] = left_sq
            left += 1
        else:
            result[pos] = right_sq
            right -= 1
        pos -= 1
    
    return result


def move_zeroes(arr):
    # Pointer for position of next non-zero element
    write_index = 0
    
    for i in range(len(arr)):
        if arr[i] != 0:
            arr[write_index], arr[i] = arr[i], arr[write_index]
            write_index += 1


def two_sum_ii(arr, target):
    left, right = 0, len(arr) - 1
    
    while left < right:
        current_sum = arr[left] + arr[right]
        
        if current_sum == target:
            return [left + 1, right + 1]  # 1-based indices
        elif current_sum < target:
            left += 1
        else:
            right -= 1
    
    return []


def three_sum_closest(nums, target):
    nums.sort()
    n = len(nums)
    closest_sum = float('inf')
    
    for i in range(n - 2):
        left, right = i + 1, n - 1
        
        while left < right:
            current_sum = nums[i] + nums[left] + nums[right]
            
            if abs(current_sum - target) < abs(closest_sum - target):
                closest_sum = current_sum
            
            if current_sum == target:
                return target  # Exact match found
            elif current_sum < target:
                left += 1
            else:
                right -= 1
    
    return closest_sum


def four_sum(nums, target):
    nums.sort()
    n = len(nums)
    result = []
    
    for i in range(n - 3):
        # Skip duplicates
        if i > 0 and nums[i] == nums[i - 1]:
            continue
        
        for j in range(i + 1, n - 2):
            # Skip duplicates
            if j > i + 1 and nums[j] == nums[j - 1]:
                continue
            
            left, right = j + 1, n - 1
            
            while left < right:
                total = nums[i] + nums[j] + nums[left] + nums[right]
                
                if total == target:
                    result.append([nums[i], nums[j], nums[left], nums[right]])
                    
                    # Skip duplicates
                    while left < right and nums[left] == nums[left + 1]:
                        left += 1
                    while left < right and nums[right] == nums[right - 1]:
                        right -= 1
                    
                    left += 1
                    right -= 1
                elif total < target:
                    left += 1
                else:
                    right -= 1
    
    return result


def reverse_words(s):
    # Split and reverse
    words = s.split()
    return ' '.join(reversed(words))


def max_water_indices(heights):
    left, right = 0, len(heights) - 1
    max_area = 0
    result = [0, 0]
    
    while left < right:
        h = min(heights[left], heights[right])
        width = right - left
        current_area = h * width
        
        if current_area > max_area:
            max_area = current_area
            result = [left, right]
        
        if heights[left] < heights[right]:
            left += 1
        else:
            right -= 1
    
    return result


def trap(height):
    if not height:
        return 0
    
    left, right = 0, len(height) - 1
    left_max, right_max = height[left], height[right]
    water = 0
    
    while left < right:
        if left_max < right_max:
            left += 1
            left_max = max(left_max, height[left])
            water += max(0, left_max - height[left])
        else:
            right -= 1
            right_max = max(right_max, height[right])
            water += max(0, right_max - height[right])
    
    return water


def find_min_rotated(nums):
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


def length_of_longest_substring_k_distinct(s, k):
    if k == 0:
        return 0
    
    char_count = {}
    left = 0
    max_length = 0
    
    for right in range(len(s)):
        # Add current character
        char_count[s[right]] = char_count.get(s[right], 0) + 1
        
        # Shrink window if more than K distinct characters
        while len(char_count) > k:
            char_count[s[left]] -= 1
            if char_count[s[left]] == 0:
                del char_count[s[left]]
            left += 1
        
        max_length = max(max_length, right - left + 1)
    
    return max_length


def subarray_sum(nums, k):
    prefix_sum = {0: 1}  # sum -> count
    current_sum = 0
    count = 0
    
    for num in nums:
        current_sum += num
        
        # Check if (current_sum - k) exists in prefix sums
        if current_sum - k in prefix_sum:
            count += prefix_sum[current_sum - k]
        
        # Update prefix sum count
        prefix_sum[current_sum] = prefix_sum.get(current_sum, 0) + 1
    
    return count
"""


# Test function for all solutions
def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING SOLUTIONS")
    print("=" * 80)
    
    # Test 1: Valid Palindrome II
    print("\n1. Valid Palindrome II:")
    test_cases = [("aba", True), ("abca", True), ("abc", False)]
    for s, expected in test_cases:
        result = valid_palindrome_ii(s)
        print(f"  '{s}' -> {result} (Expected: {expected}) {'✓' if result == expected else '✗'}")
    
    # Test 2: Sorted Squares
    print("\n2. Sorted Squares:")
    test_cases = [([-4, -1, 0, 3, 10], [0, 1, 9, 16, 100]), ([-7, -3, 2, 3, 11], [4, 9, 9, 49, 121])]
    for arr, expected in test_cases:
        result = sorted_squares(arr)
        print(f"  {arr} -> {result} {'✓' if result == expected else '✗'}")
    
    # Test 3: Move Zeroes
    print("\n3. Move Zeroes:")
    test_cases = [([0, 1, 0, 3, 12], [1, 3, 12, 0, 0]), ([0, 0, 1], [1, 0, 0])]
    for arr, expected in test_cases:
        test_arr = arr.copy()
        move_zeroes(test_arr)
        print(f"  {arr} -> {test_arr} {'✓' if test_arr == expected else '✗'}")
    
    # Test 4: Two Sum II
    print("\n4. Two Sum II:")
    test_cases = [([2, 7, 11, 15], 9, [1, 2]), ([2, 3, 4], 6, [1, 3])]
    for arr, target, expected in test_cases:
        result = two_sum_ii(arr, target)
        print(f"  {arr}, target={target} -> {result} {'✓' if result == expected else '✗'}")
    
    # Test 5: Three Sum Closest
    print("\n5. Three Sum Closest:")
    test_cases = [([-1, 2, 1, -4], 1, 2), ([0, 0, 0], 1, 0)]
    for arr, target, expected in test_cases:
        result = three_sum_closest(arr, target)
        print(f"  {arr}, target={target} -> {result} {'✓' if result == expected else '✗'}")
    
    print("\n" + "=" * 80)
    print("Testing completed!")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Two-Pointers Practice Exercises")
    print("=" * 80)
    print("\nChoose an option:")
    print("1. Test a specific problem")
    print("2. Run all tests (uncomment solutions first)")
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)
