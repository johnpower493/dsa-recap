"""
Two-Pointers Technique Implementation

The two-pointers technique is a powerful algorithmic pattern that uses two pointers
to traverse a data structure (usually an array or string) simultaneously. It's highly
efficient, typically achieving O(n) time complexity.

Common Patterns:
1. Opposite Direction: Pointers start at both ends and move toward each other
2. Same Direction: Both pointers start at beginning, one moves faster (fast & slow)
3. Sliding Window: Pointers define a window that slides across the array

Time Complexity: O(n) for most implementations
Space Complexity: O(1) for in-place operations, O(n) if creating new arrays
"""

def two_sum_sorted(arr, target):
    """
    Find two numbers in a sorted array that sum to target.
    
    Args:
        arr: Sorted list of integers
        target: Target sum to find
        
    Returns:
        list: Indices of the two numbers, or empty list if not found
        
    Example:
        arr = [2, 7, 11, 15], target = 9
        Returns: [0, 1] (2 + 7 = 9)
    """
    left = 0
    right = len(arr) - 1
    
    while left < right:
        current_sum = arr[left] + arr[right]
        
        if current_sum == target:
            return [left, right]
        elif current_sum < target:
            left += 1  # Need larger sum, move left pointer right
        else:
            right -= 1  # Need smaller sum, move right pointer left
    
    return []


def is_palindrome(s):
    """
    Check if a string is a palindrome using two pointers.
    
    Args:
        s: Input string
        
    Returns:
        bool: True if palindrome, False otherwise
        
    Example:
        "racecar" -> True
        "hello" -> False
    """
    # Convert to lowercase and remove non-alphanumeric
    cleaned = ''.join(char.lower() for char in s if char.isalnum())
    
    left = 0
    right = len(cleaned) - 1
    
    while left < right:
        if cleaned[left] != cleaned[right]:
            return False
        left += 1
        right -= 1
    
    return True


def max_area(heights):
    """
    Find the container that holds the most water (LeetCode 11).
    
    The area is determined by the shorter line and the distance between lines.
    
    Args:
        heights: List of non-negative integers representing heights
        
    Returns:
        int: Maximum area of water that can be contained
        
    Example:
        heights = [1,8,6,2,5,4,8,3,7]
        Returns: 49 (between index 1 and 8)
    """
    left = 0
    right = len(heights) - 1
    max_water = 0
    
    while left < right:
        # Calculate current area
        h = min(heights[left], heights[right])
        width = right - left
        current_area = h * width
        max_water = max(max_water, current_area)
        
        # Move the pointer with the shorter height
        if heights[left] < heights[right]:
            left += 1
        else:
            right -= 1
    
    return max_water


def remove_duplicates_sorted(arr):
    """
    Remove duplicates from a sorted array in-place.
    
    Returns the new length of the array with unique elements.
    
    Args:
        arr: Sorted list of elements
        
    Returns:
        int: Length of array after removing duplicates
        
    Example:
        arr = [1,1,2,2,3,4,4]
        Returns: 4, and arr becomes [1,2,3,4,...]
    """
    if not arr:
        return 0
    
    # Pointer for the position of last unique element
    write_index = 1
    
    for i in range(1, len(arr)):
        if arr[i] != arr[write_index - 1]:
            arr[write_index] = arr[i]
            write_index += 1
    
    return write_index


def merge_sorted_arrays(arr1, arr2):
    """
    Merge two sorted arrays into one sorted array.
    
    Args:
        arr1: First sorted array
        arr2: Second sorted array
        
    Returns:
        list: Merged sorted array
        
    Example:
        arr1 = [1,3,5], arr2 = [2,4,6]
        Returns: [1,2,3,4,5,6]
    """
    merged = []
    i, j = 0, 0
    
    while i < len(arr1) and j < len(arr2):
        if arr1[i] <= arr2[j]:
            merged.append(arr1[i])
            i += 1
        else:
            merged.append(arr2[j])
            j += 1
    
    # Add remaining elements from arr1
    while i < len(arr1):
        merged.append(arr1[i])
        i += 1
    
    # Add remaining elements from arr2
    while j < len(arr2):
        merged.append(arr2[j])
        j += 1
    
    return merged


def reverse_string(s):
    """
    Reverse a string in-place using two pointers.
    
    Args:
        s: List of characters (strings are immutable in Python, so we use list)
        
    Returns:
        None (modifies list in-place)
        
    Example:
        s = ['h','e','l','l','o']
        Becomes: ['o','l','l','e','h']
    """
    left = 0
    right = len(s) - 1
    
    while left < right:
        s[left], s[right] = s[right], s[left]
        left += 1
        right -= 1


def has_cycle_linked_list(head):
    """
    Detect if a linked list has a cycle using Floyd's cycle detection algorithm.
    
    This uses the fast and slow pointer technique.
    
    Args:
        head: Head node of linked list (assumes Node class with 'next' attribute)
        
    Returns:
        bool: True if cycle exists, False otherwise
        
    Example:
        1 -> 2 -> 3 -> 2 (cycle) -> True
        1 -> 2 -> 3 -> None -> False
    """
    if not head:
        return False
    
    slow = head
    fast = head
    
    while fast and fast.next:
        slow = slow.next  # Move one step
        fast = fast.next.next  # Move two steps
        
        if slow == fast:
            return True  # Cycle detected
    
    return False  # No cycle


def find_middle_linked_list(head):
    """
    Find the middle element of a linked list using fast and slow pointers.
    
    Args:
        head: Head node of linked list
        
    Returns:
        Node: Middle node of the linked list
        
    Example:
        1 -> 2 -> 3 -> 4 -> 5 -> Returns node with value 3
        1 -> 2 -> 3 -> 4 -> Returns node with value 2 (or 3 depending on definition)
    """
    if not head:
        return None
    
    slow = head
    fast = head
    
    # Fast moves 2 steps, slow moves 1 step
    # When fast reaches end, slow is at middle
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
    
    return slow


def three_sum(nums):
    """
    Find all unique triplets in the array that sum to zero.
    
    This combines sorting with two-pointer technique.
    
    Args:
        nums: List of integers
        
    Returns:
        list: List of triplets that sum to zero
        
    Example:
        nums = [-1, 0, 1, 2, -1, -4]
        Returns: [[-1, -1, 2], [-1, 0, 1]]
    """
    nums.sort()
    result = []
    n = len(nums)
    
    for i in range(n - 2):
        # Skip duplicates for the first element
        if i > 0 and nums[i] == nums[i - 1]:
            continue
        
        # Use two pointers for remaining two elements
        left = i + 1
        right = n - 1
        
        while left < right:
            total = nums[i] + nums[left] + nums[right]
            
            if total == 0:
                result.append([nums[i], nums[left], nums[right]])
                
                # Skip duplicates for second element
                while left < right and nums[left] == nums[left + 1]:
                    left += 1
                # Skip duplicates for third element
                while left < right and nums[right] == nums[right - 1]:
                    right -= 1
                
                left += 1
                right -= 1
            elif total < 0:
                left += 1
            else:
                right -= 1
    
    return result


def longest_substring_without_repeating(s):
    """
    Find the length of the longest substring without repeating characters.
    
    Uses sliding window technique with two pointers and a hash set.
    
    Args:
        s: Input string
        
    Returns:
        int: Maximum length of substring without repeating characters
        
    Example:
        s = "abcabcbb"
        Returns: 3 ("abc")
        s = "bbbbb"
        Returns: 1 ("b")
    """
    char_set = set()
    left = 0
    max_length = 0
    
    for right in range(len(s)):
        # If character is in set, move left pointer
        while s[right] in char_set:
            char_set.remove(s[left])
            left += 1
        
        # Add current character
        char_set.add(s[right])
        
        # Update max length
        max_length = max(max_length, right - left + 1)
    
    return max_length


def remove_element(arr, val):
    """
    Remove all occurrences of a specific value in-place.
    
    Args:
        arr: List of elements
        val: Value to remove
        
    Returns:
        int: New length of array after removals
        
    Example:
        arr = [3,2,2,3], val = 3
        Returns: 2, and arr becomes [2,2,...]
    """
    write_index = 0
    
    for i in range(len(arr)):
        if arr[i] != val:
            arr[write_index] = arr[i]
            write_index += 1
    
    return write_index


def sort_colors(arr):
    """
    Sort an array of 0s, 1s, and 2s (Dutch National Flag problem).
    Uses three pointers: low, mid, and high.
    
    Args:
        arr: List containing only 0s, 1s, and 2s
        
    Returns:
        None (modifies array in-place)
        
    Example:
        arr = [2,0,2,1,1,0]
        Becomes: [0,0,1,1,2,2]
    """
    low = 0
    mid = 0
    high = len(arr) - 1
    
    while mid <= high:
        if arr[mid] == 0:
            arr[low], arr[mid] = arr[mid], arr[low]
            low += 1
            mid += 1
        elif arr[mid] == 1:
            mid += 1
        else:  # arr[mid] == 2
            arr[mid], arr[high] = arr[high], arr[mid]
            high -= 1


# Test cases and examples
if __name__ == "__main__":
    print("=" * 70)
    print("Two-Pointers Technique - Comprehensive Examples")
    print("=" * 70)
    
    # Test 1: Two Sum (Sorted)
    print("\n" + "=" * 70)
    print("1. Two Sum in Sorted Array")
    print("=" * 70)
    arr = [2, 7, 11, 15, 17, 19]
    target = 26
    result = two_sum_sorted(arr, target)
    print(f"Array: {arr}")
    print(f"Target: {target}")
    print(f"Result indices: {result}")
    if result:
        print(f"Numbers: {arr[result[0]]} + {arr[result[1]]} = {target}")
    
    # Test 2: Palindrome
    print("\n" + "=" * 70)
    print("2. Palindrome Check")
    print("=" * 70)
    test_strings = ["racecar", "A man, a plan, a canal: Panama", "hello", "12321"]
    for s in test_strings:
        is_pal = is_palindrome(s)
        print(f"'{s}' -> {'Palindrome' if is_pal else 'Not a palindrome'}")
    
    # Test 3: Container With Most Water
    print("\n" + "=" * 70)
    print("3. Container With Most Water")
    print("=" * 70)
    heights = [1, 8, 6, 2, 5, 4, 8, 3, 7]
    print(f"Heights: {heights}")
    print(f"Maximum water area: {max_area(heights)}")
    
    # Test 4: Remove Duplicates from Sorted Array
    print("\n" + "=" * 70)
    print("4. Remove Duplicates from Sorted Array")
    print("=" * 70)
    arr = [1, 1, 2, 2, 3, 4, 4, 5, 5, 5, 6]
    print(f"Original: {arr}")
    new_length = remove_duplicates_sorted(arr)
    print(f"New length: {new_length}")
    print(f"Modified array: {arr[:new_length]}")
    
    # Test 5: Merge Sorted Arrays
    print("\n" + "=" * 70)
    print("5. Merge Two Sorted Arrays")
    print("=" * 70)
    arr1 = [1, 3, 5, 7, 9]
    arr2 = [2, 4, 6, 8, 10]
    print(f"Array 1: {arr1}")
    print(f"Array 2: {arr2}")
    merged = merge_sorted_arrays(arr1, arr2)
    print(f"Merged: {merged}")
    
    # Test 6: Reverse String
    print("\n" + "=" * 70)
    print("6. Reverse String")
    print("=" * 70)
    s = ['h', 'e', 'l', 'l', 'o']
    print(f"Original: {''.join(s)}")
    reverse_string(s)
    print(f"Reversed: {''.join(s)}")
    
    # Test 7: Three Sum
    print("\n" + "=" * 70)
    print("7. Three Sum (Triplets that sum to zero)")
    print("=" * 70)
    nums = [-1, 0, 1, 2, -1, -4]
    print(f"Array: {nums}")
    triplets = three_sum(nums)
    print(f"Triplets summing to zero: {triplets}")
    
    # Test 8: Longest Substring Without Repeating
    print("\n" + "=" * 70)
    print("8. Longest Substring Without Repeating Characters")
    print("=" * 70)
    test_cases = ["abcabcbb", "bbbbb", "pwwkew", "abcde"]
    for s in test_cases:
        length = longest_substring_without_repeating(s)
        print(f"'{s}' -> Max length: {length}")
    
    # Test 9: Remove Element
    print("\n" + "=" * 70)
    print("9. Remove Element")
    print("=" * 70)
    arr = [3, 2, 2, 3, 4, 3, 5]
    val = 3
    print(f"Original: {arr}")
    new_len = remove_element(arr, val)
    print(f"Remove all {val}s")
    print(f"New length: {new_len}")
    print(f"Modified array: {arr[:new_len]}")
    
    # Test 10: Sort Colors (Dutch National Flag)
    print("\n" + "=" * 70)
    print("10. Sort Colors (0s, 1s, and 2s)")
    print("=" * 70)
    arr = [2, 0, 2, 1, 1, 0, 1, 2, 0]
    print(f"Original: {arr}")
    sort_colors(arr)
    print(f"Sorted: {arr}")
    
    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)
