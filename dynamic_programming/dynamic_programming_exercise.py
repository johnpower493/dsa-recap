"""
Dynamic Programming - Practice Exercises (NeetCode 75 aligned)

Try solving the problems first. Solutions are included below (commented out).
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Climbing Stairs

def climbing_stairs(n):
    """Return number of ways to climb n stairs."""
    # YOUR SOLUTION HERE
    pass


# Problem 2: House Robber

def house_robber(nums):
    """Return max money without robbing adjacent houses."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: Coin Change

def coin_change(coins, amount):
    """Return minimum coins to make amount."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Longest Increasing Subsequence

def longest_increasing_subsequence(nums):
    """Return length of LIS."""
    # YOUR SOLUTION HERE
    pass


# Problem 5: Unique Paths

def unique_paths(m, n):
    """Return number of unique paths in an m x n grid."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 6: Longest Common Subsequence

def longest_common_subsequence(text1, text2):
    """Return length of LCS."""
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

"""

def climbing_stairs(n):
    if n <= 2:
        return n
    prev2, prev1 = 1, 2
    for _ in range(3, n + 1):
        prev2, prev1 = prev1, prev1 + prev2
    return prev1


def house_robber(nums):
    rob1, rob2 = 0, 0
    for n in nums:
        rob1, rob2 = rob2, max(rob2, rob1 + n)
    return rob2


def coin_change(coins, amount):
    dp = [amount + 1] * (amount + 1)
    dp[0] = 0
    for i in range(1, amount + 1):
        for c in coins:
            if i - c >= 0:
                dp[i] = min(dp[i], dp[i - c] + 1)
    return -1 if dp[amount] > amount else dp[amount]


def longest_increasing_subsequence(nums):
    if not nums:
        return 0
    dp = [1] * len(nums)
    for i in range(len(nums)):
        for j in range(i):
            if nums[j] < nums[i]:
                dp[i] = max(dp[i], dp[j] + 1)
    return max(dp)


def unique_paths(m, n):
    dp = [[1] * n for _ in range(m)]
    for r in range(1, m):
        for c in range(1, n):
            dp[r][c] = dp[r - 1][c] + dp[r][c - 1]
    return dp[m - 1][n - 1]


def longest_common_subsequence(text1, text2):
    m, n = len(text1), len(text2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if text1[i - 1] == text2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    return dp[m][n]
"""


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Dynamic Programming Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)