"""
Backtracking - Practice Exercises (NeetCode 75 aligned)

Try solving the problems first. Solutions are included below (commented out).
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Subsets

def subsets(nums):
    """Return all subsets of nums."""
    # YOUR SOLUTION HERE
    pass


# Problem 2: Subsets II (with duplicates)

def subsets_with_dup(nums):
    """Return all subsets of nums, avoiding duplicates."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: Combination Sum

def combination_sum(candidates, target):
    """Return combinations that sum to target (reuse allowed)."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Permutations

def permute(nums):
    """Return all permutations of nums."""
    # YOUR SOLUTION HERE
    pass


# Problem 5: Combination Sum II

def combination_sum2(candidates, target):
    """Return unique combinations that sum to target (each used once)."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 6: Word Search

def exist(board, word):
    """Return True if word exists in board."""
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

def subsets(nums):
    res = []

    def backtrack(i, path):
        if i == len(nums):
            res.append(path[:])
            return
        backtrack(i + 1, path)
        path.append(nums[i])
        backtrack(i + 1, path)
        path.pop()

    backtrack(0, [])
    return res


def subsets_with_dup(nums):
    nums.sort()
    res = []

    def backtrack(start, path):
        res.append(path[:])
        for i in range(start, len(nums)):
            if i > start and nums[i] == nums[i - 1]:
                continue
            path.append(nums[i])
            backtrack(i + 1, path)
            path.pop()

    backtrack(0, [])
    return res


def combination_sum(candidates, target):
    res = []

    def backtrack(start, total, path):
        if total == target:
            res.append(path[:])
            return
        if total > target:
            return
        for i in range(start, len(candidates)):
            path.append(candidates[i])
            backtrack(i, total + candidates[i], path)
            path.pop()

    backtrack(0, 0, [])
    return res


def permute(nums):
    res = []
    used = [False] * len(nums)

    def backtrack(path):
        if len(path) == len(nums):
            res.append(path[:])
            return
        for i, n in enumerate(nums):
            if used[i]:
                continue
            used[i] = True
            path.append(n)
            backtrack(path)
            path.pop()
            used[i] = False

    backtrack([])
    return res


def combination_sum2(candidates, target):
    candidates.sort()
    res = []

    def backtrack(start, total, path):
        if total == target:
            res.append(path[:])
            return
        if total > target:
            return
        for i in range(start, len(candidates)):
            if i > start and candidates[i] == candidates[i - 1]:
                continue
            path.append(candidates[i])
            backtrack(i + 1, total + candidates[i], path)
            path.pop()

    backtrack(0, 0, [])
    return res


def exist(board, word):
    if not board or not word:
        return False
    rows, cols = len(board), len(board[0])

    def backtrack(r, c, idx):
        if idx == len(word):
            return True
        if r < 0 or r >= rows or c < 0 or c >= cols:
            return False
        if board[r][c] != word[idx]:
            return False
        tmp = board[r][c]
        board[r][c] = "#"
        for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            if backtrack(r + dr, c + dc, idx + 1):
                board[r][c] = tmp
                return True
        board[r][c] = tmp
        return False

    for r in range(rows):
        for c in range(cols):
            if backtrack(r, c, 0):
                return True
    return False
"""


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Backtracking Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)