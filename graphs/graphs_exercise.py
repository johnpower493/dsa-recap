"""
Graphs - Practice Exercises (NeetCode 75 aligned)

Try solving the problems first. Solutions are included below (commented out).
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Number of Islands

def num_islands(grid):
    """Return the number of islands in a grid of '1' and '0'."""
    # YOUR SOLUTION HERE
    pass


# Problem 2: Clone Graph

def clone_graph(node):
    """Return a deep copy of an undirected graph."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 3: Course Schedule

def can_finish(num_courses, prerequisites):
    """Return True if you can finish all courses."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Pacific Atlantic Water Flow

def pacific_atlantic(heights):
    """Return coordinates that can reach both oceans."""
    # YOUR SOLUTION HERE
    pass


# Problem 5: Graph Valid Tree

def valid_tree(n, edges):
    """Return True if edges form a valid tree."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 6: Redundant Connection

def find_redundant_connection(edges):
    """Return edge that creates a cycle in an undirected graph."""
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


def num_islands(grid):
    if not grid:
        return 0
    rows, cols = len(grid), len(grid[0])
    visited = set()

    def dfs(r, c):
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
    if node is None:
        return None
    mapping = {node: type(node)(node.val)}
    queue = deque([node])
    while queue:
        cur = queue.popleft()
        for neigh in cur.neighbors:
            if neigh not in mapping:
                mapping[neigh] = type(node)(neigh.val)
                queue.append(neigh)
            mapping[cur].neighbors.append(mapping[neigh])
    return mapping[node]


def can_finish(num_courses, prerequisites):
    graph = {i: [] for i in range(num_courses)}
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


def pacific_atlantic(heights):
    if not heights:
        return []
    rows, cols = len(heights), len(heights[0])
    pac, atl = set(), set()

    def dfs(r, c, visited):
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


def valid_tree(n, edges):
    if len(edges) != n - 1:
        return False
    graph = {i: [] for i in range(n)}
    for a, b in edges:
        graph[a].append(b)
        graph[b].append(a)

    visited = set()
    stack = [0]
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        for neigh in graph[cur]:
            if neigh not in visited:
                stack.append(neigh)

    return len(visited) == n


def find_redundant_connection(edges):
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return False
        parent[rb] = ra
        return True

    for a, b in edges:
        if not union(a, b):
            return [a, b]
    return []
"""


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Graphs Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)