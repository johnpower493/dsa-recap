"""
Trees - Practice Exercises (NeetCode 75 style)

Try to solve first. Solutions are included below (commented out).
"""

# =============================================================================
# Node definition
# =============================================================================

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


# =============================================================================
# EASY
# =============================================================================

# Problem 1: Maximum Depth of Binary Tree

def maxDepth(root):
    """Return max depth of binary tree."""
    # YOUR SOLUTION HERE
    pass


# Problem 2: Invert Binary Tree

def invertTree(root):
    """Invert a binary tree and return root."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: Binary Tree Level Order Traversal

def levelOrder(root):
    """Return list of levels (BFS)."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Validate Binary Search Tree

def isValidBST(root):
    """Return True if tree is a valid BST."""
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
from collections import deque


def maxDepth(root):
    if not root:
        return 0
    return 1 + max(maxDepth(root.left), maxDepth(root.right))


def invertTree(root):
    if not root:
        return None
    root.left, root.right = invertTree(root.right), invertTree(root.left)
    return root


def levelOrder(root):
    if not root:
        return []
    res = []
    q = deque([root])
    while q:
        level_size = len(q)
        level = []
        for _ in range(level_size):
            node = q.popleft()
            level.append(node.val)
            if node.left:
                q.append(node.left)
            if node.right:
                q.append(node.right)
        res.append(level)
    return res


def isValidBST(root):
    def dfs(node, low, high):
        if not node:
            return True
        if not (low < node.val < high):
            return False
        return dfs(node.left, low, node.val) and dfs(node.right, node.val, high)

    return dfs(root, float('-inf'), float('inf'))
"""


if __name__ == "__main__":
    print("=" * 80)
    print("Trees Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)
