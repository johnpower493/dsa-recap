"""
Trees (Binary Trees/BST) - Core Patterns (NeetCode 75 aligned)

This module includes a small TreeNode class and common algorithms:
- Traversals (inorder, preorder, postorder)
- Level order (BFS)
- Max depth
- Validate BST

Time Complexity: typically O(n)
Space Complexity: O(h) recursion (h=height), or O(n) BFS queue.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple


@dataclass
class TreeNode:
    val: int
    left: Optional["TreeNode"] = None
    right: Optional["TreeNode"] = None


def inorder(root: Optional[TreeNode]) -> List[int]:
    """Left, Node, Right."""
    if not root:
        return []
    return inorder(root.left) + [root.val] + inorder(root.right)


def preorder(root: Optional[TreeNode]) -> List[int]:
    """Node, Left, Right."""
    if not root:
        return []
    return [root.val] + preorder(root.left) + preorder(root.right)


def postorder(root: Optional[TreeNode]) -> List[int]:
    """Left, Right, Node."""
    if not root:
        return []
    return postorder(root.left) + postorder(root.right) + [root.val]


def level_order(root: Optional[TreeNode]) -> List[List[int]]:
    """Breadth-first traversal by levels."""
    if not root:
        return []

    res: List[List[int]] = []
    q: Deque[TreeNode] = deque([root])

    while q:
        level_size = len(q)
        level: List[int] = []
        for _ in range(level_size):
            node = q.popleft()
            level.append(node.val)
            if node.left:
                q.append(node.left)
            if node.right:
                q.append(node.right)
        res.append(level)

    return res


def max_depth(root: Optional[TreeNode]) -> int:
    """Return maximum depth (height) of a binary tree."""
    if not root:
        return 0
    return 1 + max(max_depth(root.left), max_depth(root.right))


def is_valid_bst(root: Optional[TreeNode]) -> bool:
    """Validate BST via bounds recursion."""
    def dfs(node: Optional[TreeNode], low: float, high: float) -> bool:
        if not node:
            return True
        if not (low < node.val < high):
            return False
        return dfs(node.left, low, node.val) and dfs(node.right, node.val, high)

    return dfs(root, float("-inf"), float("inf"))


def build_example_tree() -> TreeNode:
    """Build a small example tree.

            4
          /   \
         2     6
        / \   / \
       1   3 5   7
    """
    root = TreeNode(4)
    root.left = TreeNode(2, TreeNode(1), TreeNode(3))
    root.right = TreeNode(6, TreeNode(5), TreeNode(7))
    return root


if __name__ == "__main__":
    print("=" * 70)
    print("Trees - Examples")
    print("=" * 70)

    root = build_example_tree()

    print("\n1. Traversals")
    print("inorder:  ", inorder(root))
    print("preorder: ", preorder(root))
    print("postorder:", postorder(root))

    print("\n2. Level Order")
    print(level_order(root))

    print("\n3. Max Depth")
    print(max_depth(root))

    print("\n4. Validate BST")
    print(is_valid_bst(root))
