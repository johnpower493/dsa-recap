"""
Bit Manipulation - Practice Exercises

Bit manipulation techniques are essential for low-level optimizations and solving specific algorithmic problems.
Common operations include XOR, bit shifting, counting set bits, and checking powers of two.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Single Number
# Given a non-empty array of integers nums, every element appears twice except for one.
# Find that single one. You must implement a solution with a linear runtime complexity.
def single_number(nums):
    """Returns the element that appears only once.

    Examples:
        [2,2,1] -> 1
        [4,1,2,1,2] -> 4
        [1] -> 1
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Number of 1 Bits
# Write a function that takes an unsigned integer and returns the number of '1' bits (Hamming weight).
def hamming_weight(n):
    """Returns the number of 1 bits in the binary representation of n.

    Examples:
        n=11 (binary: 1011) -> 3
        n=128 (binary: 10000000) -> 1
        n=2147483647 (binary: 01111111111111111111111111111111) -> 31
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Power of Two
# Given an integer n, return true if it is a power of two. Otherwise, return false.
def is_power_of_two(n):
    """Returns True if n is a power of two.

    Examples:
        1 -> True
        16 -> True
        3 -> False
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Single Number II
# Given an integer array nums where every element appears three times except for one,
# which appears exactly once. Find the single element and return it.
def single_number_ii(nums):
    """Returns the element that appears only once.

    Examples:
        [2,2,3,2] -> 3
        [0,1,0,1,0,1,99] -> 99
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Counting Bits
# Given an integer n, return an array ans of length n + 1 such that for each i (0 <= i <= n),
# ans[i] is the number of 1's in the binary representation of i.
def count_bits(n):
    """Returns an array where ans[i] = number of 1 bits in i.

    Examples:
        n=2 -> [0,1,1]
        n=5 -> [0,1,1,2,1,2]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Reverse Bits
# Reverse bits of a given 32 bits unsigned integer.
def reverse_bits(n):
    """Returns the integer with reversed bits.

    Examples:
        n=0b00000010100101000001111010011100 -> 0b00111001011110000010100101000000
        n=0b11111111111111111111111111111101 -> 0b10111111111111111111111111111111
    """
    # YOUR SOLUTION HERE
    pass


# Problem 7: Missing Number
# Given an array nums containing n distinct numbers in the range [0, n],
# return the only number in the range that is missing from the array.
def missing_number(nums):
    """Returns the missing number.

    Examples:
        [3,0,1] -> 2
        [0,1] -> 2
        [9,6,4,2,3,5,7,0,1] -> 8
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 8: Single Number III
# Given an integer array nums, in which exactly two elements appear only once and
# all the other elements appear exactly twice. Find the two elements that appear only once.
def single_number_iii(nums):
    """Returns the two elements that appear only once.

    Examples:
        [1,2,1,3,2,5] -> [3,5] (order doesn't matter)
        [-1,0] -> [-1,0]
        [0,1] -> [0,1]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 9: Maximum XOR of Two Numbers in an Array
# Given an integer array nums, return the maximum result of nums[i] XOR nums[j], where 0 <= i <= j < n.
def find_maximum_xor(nums):
    """Returns the maximum XOR of any two numbers in the array.

    Examples:
        [3,10,5,25,2,8] -> 28 (5 XOR 25 = 28)
        [0] -> 0
        [2,4] -> 6
    """
    # YOUR SOLUTION HERE
    pass


# Problem 10: Bitwise AND of Numbers Range
# Given two integers left and right, return the bitwise AND of all numbers in the range [left, right].
def range_bitwise_and(left, right):
    """Returns the bitwise AND of all numbers in [left, right].

    Examples:
        left=5, right=7 -> 4
        left=0, right=0 -> 0
        left=1, right=2147483647 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 11: Sum of Two Integers
# Given two integers a and b, return the sum of the two integers without using the + and - operators.
def get_sum(a, b):
    """Returns the sum of a and b without using + or -.

    Examples:
        a=1, b=2 -> 3
        a=2, b=3 -> 5
        a=-1, b=1 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# Problem 12: Find the Difference
# You are given two strings s and t. String t is generated by random shuffling string s
# and then adding one more letter at a random position. Return the letter that was added to t.
def find_the_difference(s, t):
    """Returns the letter that was added to t.

    Examples:
        s="abcd", t="abcde" -> "e"
        s="", t="y" -> "y"
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
def single_number(nums):
    """Using XOR - O(n) time, O(1) space"""
    result = 0
    for num in nums:
        result ^= num
    return result


def hamming_weight(n):
    """Count set bits - O(1) time (at most 32 bits), O(1) space"""
    count = 0
    while n:
        n &= n - 1  # Clear the least significant set bit
        count += 1
    return count


def is_power_of_two(n):
    """Check if n has exactly one bit set - O(1) time"""
    return n > 0 and (n & (n - 1)) == 0


def single_number_ii(nums):
    """Count bits modulo 3 - O(n) time, O(1) space"""
    result = 0
    for i in range(32):
        bit_sum = 0
        for num in nums:
            bit_sum += (num >> i) & 1
        if bit_sum % 3:
            result |= (1 << i)
    
    # Handle negative numbers (Python uses unlimited precision)
    if result >= 2**31:
        result -= 2**32
    
    return result


def count_bits(n):
    """Dynamic programming using LSB - O(n) time, O(n) space"""
    dp = [0] * (n + 1)
    for i in range(1, n + 1):
        # dp[i] = dp[i >> 1] + (i & 1)
        # Or: dp[i] = dp[i & (i - 1)] + 1 (remove the rightmost set bit)
        dp[i] = dp[i & (i - 1)] + 1
    return dp


def reverse_bits(n):
    """Reverse 32 bits - O(1) time"""
    result = 0
    for _ in range(32):
        result = (result << 1) | (n & 1)
        n >>= 1
    return result


def missing_number(nums):
    """XOR approach - O(n) time, O(1) space"""
    result = len(nums)
    for i, num in enumerate(nums):
        result ^= i ^ num
    return result


def single_number_iii(nums):
    """XOR + partition - O(n) time, O(1) space"""
    # XOR all numbers to get xor of the two unique numbers
    xor_all = 0
    for num in nums:
        xor_all ^= num
    
    # Find the rightmost set bit (differentiating bit)
    diff_bit = xor_all & -xor_all
    
    # Partition numbers based on diff_bit and find each unique number
    num1 = num2 = 0
    for num in nums:
        if num & diff_bit:
            num1 ^= num
        else:
            num2 ^= num
    
    return [num1, num2]


def find_maximum_xor(nums):
    """Bitmask + hash set - O(n * 32) time, O(1) space"""
    max_xor = 0
    mask = 0
    
    for i in range(31, -1, -1):
        mask |= (1 << i)
        prefixes = {num & mask for num in nums}
        
        # Try to set this bit in max_xor
        candidate = max_xor | (1 << i)
        
        # Check if we can achieve candidate
        for prefix in prefixes:
            if (candidate ^ prefix) in prefixes:
                max_xor = candidate
                break
    
    return max_xor


def range_bitwise_and(left, right):
    """Find common prefix - O(1) time"""
    shift = 0
    while left != right:
        left >>= 1
        right >>= 1
        shift += 1
    return left << shift


def get_sum(a, b):
    """Using XOR for sum, AND for carry - O(1) time"""
    mask = 0xFFFFFFFF
    
    while b != 0:
        # Calculate sum without carry
        sum_without_carry = (a ^ b) & mask
        
        # Calculate carry
        carry = ((a & b) << 1) & mask
        
        a = sum_without_carry
        b = carry
    
    # Handle overflow for 32-bit integers
    if a > 0x7FFFFFFF:
        a = ~(a ^ mask)
    
    return a


def find_the_difference(s, t):
    """XOR all characters - O(n) time, O(1) space"""
    result = 0
    for char in s:
        result ^= ord(char)
    for char in t:
        result ^= ord(char)
    return chr(result)
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Bit Manipulation Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)