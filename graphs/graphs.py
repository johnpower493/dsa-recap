"""
Graphs - Core Patterns (NeetCode 75 aligned)

Common patterns:
- DFS/BFS traversal
- Cycle detection (directed/undirected)
- Topological sort (course schedule)
- Counting connected components
- Pacific Atlantic Water Flow

Time Complexity: often O(V + E)
Space Complexity: O(V + E)
"""

from __future__ import annotations

from collections import deque
from typing import Dict, List, Set, Tuple


def num_islands(grid: List[List[str]]) -> int:
    """Count islands of '1's in a 2D grid.

    Example:
        [
          ["1","1","0"],
          ["1","0","0"],
          ["0","0","1"],
        ] -> 2
    """
    if not grid:
        return 0

    rows, cols = len(grid), len(grid[0])
    visited: Set[Tuple[int, int]] = set()

    def dfs(r: int, c: int) -> None:
        stack = [(r, c)]
        while stack:
            cr, cc = stack.pop()
            if (cr, cc) in visited:
                continue
            visited.add((cr, cc))
            for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                nr, nc = cr + dr, cc + dc
                if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] == "1":
                    stack.append((nr, nc))

    count = 0
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == "1" and (r, c) not in visited:
                count += 1
                dfs(r, c)
    return count


def clone_graph(node):
    """Clone an undirected graph. Node has val + neighbors list.

    Uses BFS and a hashmap from original to cloned nodes.
    """
    if node is None:
        return None

    mapping: Dict[object, object] = {node: type(node)(node.val)}
    queue = deque([node])

    while queue:
        cur = queue.popleft()
        for neigh in cur.neighbors:
            if neigh not in mapping:
                mapping[neigh] = type(node)(neigh.val)
                queue.append(neigh)
            mapping[cur].neighbors.append(mapping[neigh])

    return mapping[node]


def can_finish(num_courses: int, prerequisites: List[List[int]]) -> bool:
    """Return True if all courses can be finished (no directed cycle)."""
    graph: Dict[int, List[int]] = {i: [] for i in range(num_courses)}
    indeg = [0] * num_courses

    for nxt, pre in prerequisites:
        graph[pre].append(nxt)
        indeg[nxt] += 1

    queue = deque([i for i, d in enumerate(indeg) if d == 0])
    taken = 0

    while queue:
        cur = queue.popleft()
        taken += 1
        for nxt in graph[cur]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)

    return taken == num_courses


def pacific_atlantic(heights: List[List[int]]) -> List[List[int]]:
    """Return coords that can flow to both Pacific and Atlantic.

    Reverse DFS from borders of each ocean.
    """
    if not heights:
        return []

    rows, cols = len(heights), len(heights[0])
    pac = set()
    atl = set()

    def dfs(r: int, c: int, visited: Set[Tuple[int, int]]) -> None:
        stack = [(r, c)]
        while stack:
            cr, cc = stack.pop()
            if (cr, cc) in visited:
                continue
            visited.add((cr, cc))
            for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                nr, nc = cr + dr, cc + dc
                if 0 <= nr < rows and 0 <= nc < cols:
                    if heights[nr][nc] >= heights[cr][cc]:
                        stack.append((nr, nc))

    for c in range(cols):
        dfs(0, c, pac)
        dfs(rows - 1, c, atl)
    for r in range(rows):
        dfs(r, 0, pac)
        dfs(r, cols - 1, atl)

    return [[r, c] for (r, c) in pac & atl]


if __name__ == "__main__":
    print("=" * 70)
    print("Graphs - Examples")
    print("=" * 70)

    grid = [
        ["1", "1", "0"],
        ["1", "0", "0"],
        ["0", "0", "1"],
    ]
    print("\n1. Number of Islands")
    print(num_islands(grid))

    print("\n2. Course Schedule")
    print(can_finish(2, [[1, 0]]))
    print(can_finish(2, [[1, 0], [0, 1]]))

    print("\n3. Pacific Atlantic Water Flow")
    heights = [
        [1, 2, 2, 3, 5],
        [3, 2, 3, 4, 4],
        [2, 4, 5, 3, 1],
        [6, 7, 1, 4, 5],
        [5, 1, 1, 2, 4],
    ]
    print(pacific_atlantic(heights))