"""
Binary Search Algorithm Implementation

Binary search is an efficient algorithm for finding an item from a sorted list of items.
It works by repeatedly dividing in half the portion of the list that could contain the item,
until you've narrowed down the possible locations to just one.

Time Complexity: O(log n)
Space Complexity: O(1)
"""

def binary_search(arr, target):
    """
    Perform binary search on a sorted array.
    
    Args:
        arr: A sorted list of elements
        target: The element to search for
        
    Returns:
        int: Index of target if found, -1 otherwise
    """
    left = 0
    right = len(arr) - 1
    
    while left <= right:
        # Calculate middle index
        mid = left + (right - left) // 2
        
        # Check if target is present at mid
        if arr[mid] == target:
            return mid
        
        # If target is greater, ignore left half
        elif arr[mid] < target:
            left = mid + 1
        
        # If target is smaller, ignore right half
        else:
            right = mid - 1
    
    # Target was not found
    return -1


def binary_search_recursive(arr, target, left=0, right=None):
    """
    Perform binary search recursively.
    
    Args:
        arr: A sorted list of elements
        target: The element to search for
        left: Left boundary (default 0)
        right: Right boundary (default len(arr) - 1)
        
    Returns:
        int: Index of target if found, -1 otherwise
    """
    if right is None:
        right = len(arr) - 1
    
    if left > right:
        return -1
    
    mid = left + (right - left) // 2
    
    if arr[mid] == target:
        return mid
    elif arr[mid] < target:
        return binary_search_recursive(arr, target, mid + 1, right)
    else:
        return binary_search_recursive(arr, target, left, mid - 1)


def find_first_occurrence(arr, target):
    """
    Find the first occurrence of target in a sorted array.
    
    Args:
        arr: A sorted list of elements (may contain duplicates)
        target: The element to search for
        
    Returns:
        int: Index of first occurrence if found, -1 otherwise
    """
    result = -1
    left, right = 0, len(arr) - 1
    
    while left <= right:
        mid = left + (right - left) // 2
        
        if arr[mid] == target:
            result = mid
            right = mid - 1  # Continue searching in left half
        elif arr[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    
    return result


def find_last_occurrence(arr, target):
    """
    Find the last occurrence of target in a sorted array.
    
    Args:
        arr: A sorted list of elements (may contain duplicates)
        target: The element to search for
        
    Returns:
        int: Index of last occurrence if found, -1 otherwise
    """
    result = -1
    left, right = 0, len(arr) - 1
    
    while left <= right:
        mid = left + (right - left) // 2
        
        if arr[mid] == target:
            result = mid
            left = mid + 1  # Continue searching in right half
        elif arr[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    
    return result


def find_insert_position(arr, target):
    """
    Find the position where target should be inserted to maintain sorted order.
    
    Args:
        arr: A sorted list of elements
        target: The element to insert
        
    Returns:
        int: Index where target should be inserted
    """
    left, right = 0, len(arr)
    
    while left < right:
        mid = left + (right - left) // 2
        
        if arr[mid] < target:
            left = mid + 1
        else:
            right = mid
    
    return left


# Test cases and examples
if __name__ == "__main__":
    # Basic test array
    arr = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
    
    print("=" * 60)
    print("Binary Search Algorithm Examples")
    print("=" * 60)
    print(f"Array: {arr}")
    print()
    
    # Test 1: Element present in array
    target = 23
    result = binary_search(arr, target)
    print(f"Test 1: Searching for {target}")
    print(f"  Iterative: Found at index {result}")
    result_recursive = binary_search_recursive(arr, target)
    print(f"  Recursive: Found at index {result_recursive}")
    print()
    
    # Test 2: Element not present in array
    target = 42
    result = binary_search(arr, target)
    print(f"Test 2: Searching for {target}")
    print(f"  Iterative: Not found (returned {result})")
    result_recursive = binary_search_recursive(arr, target)
    print(f"  Recursive: Not found (returned {result_recursive})")
    print()
    
    # Test 3: Search at boundaries
    target = 2
    result = binary_search(arr, target)
    print(f"Test 3: Searching for first element {target}")
    print(f"  Found at index {result}")
    
    target = 91
    result = binary_search(arr, target)
    print(f"Test 4: Searching for last element {target}")
    print(f"  Found at index {result}")
    print()
    
    # Test 4: Array with duplicates
    arr_with_dupes = [1, 2, 2, 2, 3, 4, 4, 5, 6]
    print("=" * 60)
    print("Searching in array with duplicates")
    print(f"Array: {arr_with_dupes}")
    print()
    
    target = 2
    first = find_first_occurrence(arr_with_dupes, target)
    last = find_last_occurrence(arr_with_dupes, target)
    print(f"Target: {target}")
    print(f"  First occurrence at index: {first}")
    print(f"  Last occurrence at index: {last}")
    print(f"  Total occurrences: {last - first + 1 if first != -1 else 0}")
    print()
    
    # Test 5: Find insert position
    print("=" * 60)
    print("Finding Insert Position")
    print(f"Array: {arr}")
    print()
    
    targets = [1, 10, 50, 100]
    for target in targets:
        pos = find_insert_position(arr, target)
        print(f"  Insert {target} at position {pos}")
    print()
    
    # Test 6: Empty array edge case
    print("=" * 60)
    print("Edge Case: Empty Array")
    empty_arr = []
    result = binary_search(empty_arr, 5)
    print(f"Searching in empty array for 5: {result}")
    print()
    
    # Test 7: Single element array
    print("Edge Case: Single Element Array")
    single_arr = [42]
    result = binary_search(single_arr, 42)
    print(f"Searching for 42: Found at index {result}")
    result = binary_search(single_arr, 10)
    print(f"Searching for 10: {result}")
