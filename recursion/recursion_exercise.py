"""
Recursion - Practice Exercises

Recursion is a method of solving a problem where the solution depends on solutions
to smaller instances of the same problem. Key concepts include base cases and recursive cases.

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 1: Factorial
# Implement the factorial function recursively.
def factorial(n):
    """Returns n! (factorial of n).

    Examples:
        n=0 -> 1
        n=5 -> 120
        n=3 -> 6
    """
    # YOUR SOLUTION HERE
    pass


# Problem 2: Fibonacci Number
# Calculate the nth Fibonacci number recursively.
def fibonacci(n):
    """Returns the nth Fibonacci number.

    Examples:
        n=0 -> 0
        n=1 -> 1
        n=5 -> 5
        n=10 -> 55
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Sum of Digits
# Given an integer, return the sum of its digits.
def sum_of_digits(n):
    """Returns the sum of all digits in n.

    Examples:
        n=123 -> 6
        n=999 -> 27
        n=0 -> 0
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Power Function
# Implement pow(x, n) recursively.
def power(x, n):
    """Returns x raised to the power n.

    Examples:
        x=2, n=10 -> 1024
        x=2, n=3 -> 8
        x=3, n=4 -> 81
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Reverse String
# Reverse a string using recursion.
def reverse_string(s):
    """Returns the reverse of string s.

    Examples:
        "hello" -> "olleh"
        "abc" -> "cba"
        "" -> ""
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Is Palindrome
# Check if a string is a palindrome using recursion.
def is_palindrome(s):
    """Returns True if s is a palindrome.

    Examples:
        "racecar" -> True
        "hello" -> False
        "a" -> True
        "" -> True
    """
    # YOUR SOLUTION HERE
    pass


# Problem 7: Binary Search
# Implement binary search recursively.
def binary_search(arr, target):
    """Returns the index of target in arr, or -1 if not found.

    Examples:
        arr=[1,2,3,4,5,6,7], target=5 -> 4
        arr=[1,2,3,4,5,6,7], target=1 -> 0
        arr=[1,2,3,4,5,6,7], target=8 -> -1
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 8: Generate Parentheses
# Given n pairs of parentheses, generate all combinations of well-formed parentheses.
def generate_parentheses(n):
    """Returns all valid combinations of n pairs of parentheses.

    Examples:
        n=1 -> ["()"]
        n=2 -> ["(())","()()"]
        n=3 -> ["((()))","(()())","(())()","()(())","()()()"]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 9: Subsets
# Given an integer array nums, return all possible subsets (the power set).
def subsets(nums):
    """Returns all possible subsets of nums.

    Examples:
        nums=[1,2,3] -> [[],[1],[2],[1,2],[3],[1,3],[2,3],[1,2,3]]
        nums=[0] -> [[],[0]]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 10: Permutations
# Given an array nums of distinct integers, return all possible permutations.
def permutations(nums):
    """Returns all possible permutations of nums.

    Examples:
        nums=[1,2,3] -> [[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]
        nums=[0,1] -> [[0,1],[1,0]]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 11: Combination Sum
# Given an array of distinct integers candidates and a target integer target,
# return a list of all unique combinations of candidates where the chosen numbers sum to target.
def combination_sum(candidates, target):
    """Returns all unique combinations that sum to target.

    Examples:
        candidates=[2,3,6,7], target=7 -> [[2,2,3],[7]]
        candidates=[2,3,5], target=8 -> [[2,2,2,2],[2,3,3],[3,5]]
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 12: Letter Combinations of a Phone Number
# Given a string containing digits from 2-9, return all possible letter combinations.
def letter_combinations(digits):
    """Returns all possible letter combinations for the phone number.

    Examples:
        digits="23" -> ["ad","ae","af","bd","be","bf","cd","ce","cf"]
        digits="" -> []
        digits="2" -> ["a","b","c"]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 13: N-Queens
# The n-queens puzzle is the problem of placing n queens on an n x n chessboard.
# Return all distinct solutions to the n-queens puzzle.
def solve_n_queens(n):
    """Returns all distinct solutions to the n-queens puzzle.

    Examples:
        n=4 -> [[".Q..","...Q","Q...","..Q."],["..Q.","Q...","...Q",".Q.."]]
        n=1 -> [["Q"]]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 14: Climbing Stairs
# You are climbing a staircase. It takes n steps to reach the top.
# Each time you can either climb 1 or 2 steps. In how many distinct ways can you climb to the top?
def climb_stairs(n):
    """Returns the number of distinct ways to climb n stairs.

    Examples:
        n=2 -> 2 (1+1 or 2)
        n=3 -> 3 (1+1+1, 1+2, or 2+1)
        n=4 -> 5
    """
    # YOUR SOLUTION HERE
    pass


# Problem 15: Unique Paths
# There is a robot on an m x n grid. The robot can only move either down or right.
# Find all possible unique paths from top-left to bottom-right.
def unique_paths(m, n):
    """Returns the number of unique paths from (0,0) to (m-1,n-1).

    Examples:
        m=3, n=7 -> 28
        m=3, n=2 -> 3
        m=1, n=1 -> 1
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
def factorial(n):
    """Basic recursion - O(n) time, O(n) space"""
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)


def fibonacci(n):
    """Basic recursion - O(2^n) time, O(n) space"""
    if n == 0:
        return 0
    if n == 1:
        return 1
    return fibonacci(n - 1) + fibonacci(n - 2)


def sum_of_digits(n):
    """Tail recursion - O(log n) time, O(log n) space"""
    if n == 0:
        return 0
    return (n % 10) + sum_of_digits(n // 10)


def power(x, n):
    """Fast exponentiation - O(log n) time, O(log n) space"""
    if n == 0:
        return 1
    if n < 0:
        return 1 / power(x, -n)
    
    half = power(x, n // 2)
    if n % 2 == 0:
        return half * half
    else:
        return half * half * x


def reverse_string(s):
    """Simple recursion - O(n) time, O(n) space"""
    if len(s) <= 1:
        return s
    return reverse_string(s[1:]) + s[0]


def is_palindrome(s):
    """Two-pointer recursion - O(n) time, O(n) space"""
    if len(s) <= 1:
        return True
    if s[0] != s[-1]:
        return False
    return is_palindrome(s[1:-1])


def binary_search(arr, target):
    """Divide and conquer - O(log n) time, O(log n) space"""
    def binary_search_helper(left, right):
        if left > right:
            return -1
        
        mid = (left + right) // 2
        
        if arr[mid] == target:
            return mid
        elif arr[mid] < target:
            return binary_search_helper(mid + 1, right)
        else:
            return binary_search_helper(left, mid - 1)
    
    return binary_search_helper(0, len(arr) - 1)


def generate_parentheses(n):
    """Backtracking - O(4^n / sqrt(n)) time, O(n) space"""
    result = []
    
    def backtrack(current, open_count, close_count):
        if len(current) == 2 * n:
            result.append(current)
            return
        
        if open_count < n:
            backtrack(current + '(', open_count + 1, close_count)
        
        if close_count < open_count:
            backtrack(current + ')', open_count, close_count + 1)
    
    backtrack('', 0, 0)
    return result


def subsets(nums):
    """Backtracking - O(2^n) time, O(n) space"""
    result = []
    
    def backtrack(start, current):
        result.append(current[:])
        
        for i in range(start, len(nums)):
            current.append(nums[i])
            backtrack(i + 1, current)
            current.pop()
    
    backtrack(0, [])
    return result


def permutations(nums):
    """Backtracking - O(n! * n) time, O(n) space"""
    result = []
    
    def backtrack(used, current):
        if len(current) == len(nums):
            result.append(current[:])
            return
        
        for i in range(len(nums)):
            if not used[i]:
                used[i] = True
                current.append(nums[i])
                backtrack(used, current)
                current.pop()
                used[i] = False
    
    backtrack([False] * len(nums), [])
    return result


def combination_sum(candidates, target):
    """Backtracking - O(n^(t/min)) time, O(t) space"""
    result = []
    
    def backtrack(start, current, remaining):
        if remaining == 0:
            result.append(current[:])
            return
        if remaining < 0:
            return
        
        for i in range(start, len(candidates)):
            current.append(candidates[i])
            backtrack(i, current, remaining - candidates[i])
            current.pop()
    
    backtrack(0, [], target)
    return result


def letter_combinations(digits):
    """Backtracking - O(4^n) time, O(n) space"""
    if not digits:
        return []
    
    phone_map = {
        '2': 'abc',
        '3': 'def',
        '4': 'ghi',
        '5': 'jkl',
        '6': 'mno',
        '7': 'pqrs',
        '8': 'tuv',
        '9': 'wxyz'
    }
    
    result = []
    
    def backtrack(index, current):
        if index == len(digits):
            result.append(''.join(current))
            return
        
        for letter in phone_map[digits[index]]:
            current.append(letter)
            backtrack(index + 1, current)
            current.pop()
    
    backtrack(0, [])
    return result


def solve_n_queens(n):
    """Backtracking - O(n!) time, O(n) space"""
    result = []
    
    def is_safe(row, col, queens):
        for r, c in enumerate(queens):
            if c == col or abs(row - r) == abs(col - c):
                return False
        return True
    
    def backtrack(row, queens):
        if row == n:
            board = ['.' * c + 'Q' + '.' * (n - c - 1) for c in queens]
            result.append(board)
            return
        
        for col in range(n):
            if is_safe(row, col, queens):
                queens.append(col)
                backtrack(row + 1, queens)
                queens.pop()
    
    backtrack(0, [])
    return result


def climb_stairs(n):
    """Recursion with memoization - O(n) time, O(n) space"""
    memo = {}
    
    def climb(n):
        if n in memo:
            return memo[n]
        if n <= 2:
            return n
        
        memo[n] = climb(n - 1) + climb(n - 2)
        return memo[n]
    
    return climb(n)


def unique_paths(m, n):
    """Recursion with memoization - O(m*n) time, O(m*n) space"""
    memo = {}
    
    def paths(row, col):
        if row == m - 1 or col == n - 1:
            return 1
        if (row, col) in memo:
            return memo[(row, col)]
        
        memo[(row, col)] = paths(row + 1, col) + paths(row, col + 1)
        return memo[(row, col)]
    
    return paths(0, 0)
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Recursion Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)