"""
Union-Find (Disjoint Set Union) - Practice Exercises

Union-Find is a data structure that keeps track of elements partitioned into
disjoint (non-overlapping) sets. It supports two primary operations:
- Find: Determine which set a particular element is in
- Union: Join two sets together

SOLUTIONS are included below each problem (commented out).
"""

# =============================================================================
# BASIC IMPLEMENTATION
# =============================================================================

# Problem 1: Basic Union-Find
# Implement a basic Union-Find data structure with path compression and union by rank.
class UnionFind:
    def __init__(self, n):
        """Initialize with n elements (0 to n-1)."""
        # YOUR SOLUTION HERE
        pass
    
    def find(self, x):
        """Find the root/representative of the set containing x."""
        # YOUR SOLUTION HERE
        pass
    
    def union(self, x, y):
        """Merge the sets containing x and y."""
        # YOUR SOLUTION HERE
        pass
    
    def connected(self, x, y):
        """Check if x and y are in the same set."""
        # YOUR SOLUTION HERE
        pass


# =============================================================================
# EASY PROBLEMS
# =============================================================================

# Problem 2: Number of Connected Components in an Undirected Graph
# Given n nodes labeled from 0 to n - 1 and a list of undirected edges,
# write a function to find the number of connected components in an undirected graph.
def count_components(n, edges):
    """Returns the number of connected components.

    Examples:
        n=5, edges=[[0,1],[1,2],[3,4]] -> 2
        n=4, edges=[[0,1],[2,3],[1,2]] -> 1
    """
    # YOUR SOLUTION HERE
    pass


# Problem 3: Find if Path Exists in Graph
# There is a bi-directional graph with n vertices, where each vertex is labeled from 0 to n - 1.
# Given the graph edges and the start and end vertices, check if there is a valid path.
def valid_path(n, edges, start, end):
    """Returns True if there is a path from start to end.

    Examples:
        n=3, edges=[[0,1],[1,2],[2,0]], start=0, end=2 -> True
        n=6, edges=[[0,1],[0,2],[3,5],[5,4],[4,3]], start=0, end=5 -> False
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM PROBLEMS
# =============================================================================

# Problem 4: Redundant Connection
# In this problem, a tree is an undirected graph that is connected and has no cycles.
# Return an edge that can be removed so that the resulting graph is a tree.
def find_redundant_connection(edges):
    """Returns an edge that can be removed to form a tree.

    Examples:
        edges=[[1,2],[1,3],[2,3]] -> [2,3]
        edges=[[1,2],[2,3],[3,4],[1,4],[1,5]] -> [1,4]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 5: Satisfiability of Equality Equations
# Given an array equations of strings that represent relationships between variables,
# return true if and only if all equations are satisfied.
def equations_possible(equations):
    """Returns True if all equations are satisfiable.

    Examples:
        equations=["a==b","b!=a"] -> False
        equations=["b==a","a==b"] -> True
    """
    # YOUR SOLUTION HERE
    pass


# Problem 6: Longest Consecutive Sequence
# Given an unsorted array of integers nums, return the length of the longest consecutive elements sequence.
def longest_consecutive(nums):
    """Returns the length of the longest consecutive sequence.

    Examples:
        [100,4,200,1,3,2] -> 4 (sequence: [1,2,3,4])
        [0,3,7,2,5,8,4,6,0,1] -> 9
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD PROBLEMS
# =============================================================================

# Problem 7: Accounts Merge
# Given a list of accounts where each element accounts[i] is a list of strings,
# merge accounts that belong to the same person.
def accounts_merge(accounts):
    """Returns the merged accounts.

    Examples:
        accounts=[["John","johnsmith@mail.com","john_newyork@mail.com"],
                 ["John","johnsmith@mail.com","john00@mail.com"],
                 ["Mary","mary@mail.com"],
                 ["John","johnnybravo@mail.com"]]
        -> [["John","john00@mail.com","john_newyork@mail.com","johnsmith@mail.com"],
            ["Mary","mary@mail.com"],
            ["John","johnnybravo@mail.com"]]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 8: Number of Islands II
# You are given an empty 2D grid grid of size m x n. You need to perform k operations.
# Each operation is at position (row, col), turning the cell into a land (1).
# Return an array of integers answer where answer[i] is the number of islands after the i-th operation.
def num_islands2(m, n, positions):
    """Returns the number of islands after each operation.

    Examples:
        m=3, n=3, positions=[[0,0],[0,1],[1,2],[2,1],[1,1]]
        -> [1,1,2,3,4]
    """
    # YOUR SOLUTION HERE
    pass


# Problem 9: Number of Operations to Make Network Connected
# There are n computers numbered from 0 to n-1 connected by ethernet cables.
# You are given the array connections where connections[i] = [a, b] represents a connection.
# If you can remove some cables and move the remaining cables to connect all computers,
# return the minimum number of times you need to move cables. Otherwise, return -1.
def make_connected(n, connections):
    """Returns the minimum number of cable moves, or -1 if impossible.

    Examples:
        n=4, connections=[[0,1],[0,2],[1,2]] -> 1
        n=6, connections=[[0,1],[0,2],[0,3],[1,2],[1,3]] -> 2
    """
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# BONUS PROBLEMS
# =============================================================================

# Problem 10: Lexicographically Smallest String After Applying Operations
# You are given a string s of even length and an integer k. You can perform the following operations:
# - Choose an index i in the range [0, n-1] and flip the character at that index.
# After performing exactly k operations, return the lexicographically smallest string.
def smallest_string(s, k):
    """Returns the lexicographically smallest string after k operations.

    Examples:
        s="4321", k=4 -> "1342"
    """
    # YOUR SOLUTION HERE
    pass


# Problem 11: Removing Minimum Number of Magic Edges
# Given a tree with n nodes labeled from 0 to n-1 and an array edges where edges[i] = [a, b],
# each edge is colored either red (0) or blue (1). Return the minimum number of edges
# that must be removed to make all paths in the tree consist of edges of the same color.
def max_num_edges_to_remove(n, edges):
    """Returns the minimum number of edges to remove.

    Examples:
        n=4, edges=[[0,1,1],[1,2,0],[1,3,0]] -> 0
        n=4, edges=[[0,1,1],[1,2,1],[2,3,1]] -> 2
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
class UnionFind:
    """Union-Find with path compression and union by rank"""
    
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x):
        """Find with path compression - O(alpha(n)) amortized"""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]
    
    def union(self, x, y):
        """Union by rank - O(alpha(n)) amortized"""
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return  # Already in the same set
        
        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1
    
    def connected(self, x, y):
        """Check if x and y are in the same set"""
        return self.find(x) == self.find(y)


def count_components(n, edges):
    """Union-Find - O(V + E * alpha(V)) time"""
    uf = UnionFind(n)
    
    for u, v in edges:
        uf.union(u, v)
    
    # Count unique roots
    roots = set(uf.find(i) for i in range(n))
    return len(roots)


def valid_path(n, edges, start, end):
    """Union-Find - O(V + E * alpha(V)) time"""
    uf = UnionFind(n)
    
    for u, v in edges:
        uf.union(u, v)
    
    return uf.connected(start, end)


def find_redundant_connection(edges):
    """Union-Find - O(E * alpha(V)) time"""
    n = len(edges)
    uf = UnionFind(n + 1)  # Nodes are 1-indexed
    
    for u, v in edges:
        if uf.connected(u, v):
            return [u, v]  # This edge creates a cycle
        uf.union(u, v)
    
    return []


def equations_possible(equations):
    """Union-Find - O(n * alpha(V)) time"""
    uf = UnionFind(26)  # 26 letters
    
    # First pass: handle all equality constraints
    for eq in equations:
        if eq[1] == '=':
            uf.union(ord(eq[0]) - ord('a'), ord(eq[3]) - ord('a'))
    
    # Second pass: check all inequality constraints
    for eq in equations:
        if eq[1] == '!':
            if uf.connected(ord(eq[0]) - ord('a'), ord(eq[3]) - ord('a')):
                return False
    
    return True


def longest_consecutive(nums):
    """Union-Find or Set - O(n) time"""
    if not nums:
        return 0
    
    num_set = set(nums)
    max_length = 0
    
    for num in num_set:
        # Only start counting if num is the start of a sequence
        if num - 1 not in num_set:
            current_num = num
            current_length = 1
            
            while current_num + 1 in num_set:
                current_num += 1
                current_length += 1
            
            max_length = max(max_length, current_length)
    
    return max_length


def accounts_merge(accounts):
    """Union-Find + Hash Map - O(n * alpha(n)) time"""
    email_to_name = {}
    email_to_id = {}
    id_to_emails = {}
    
    uf = UnionFind(len(accounts))
    
    # Assign IDs to emails and build union-find structure
    for i, account in enumerate(accounts):
        name = account[0]
        for email in account[1:]:
            email_to_name[email] = name
            if email not in email_to_id:
                email_to_id[email] = i
            uf.union(i, email_to_id[email])
    
    # Group emails by root ID
    for email, id in email_to_id.items():
        root = uf.find(id)
        if root not in id_to_emails:
            id_to_emails[root] = []
        id_to_emails[root].append(email)
    
    # Build result
    result = []
    for root, emails in id_to_emails.items():
        name = email_to_name[emails[0]]
        merged_account = [name] + sorted(emails)
        result.append(merged_account)
    
    return result


def num_islands2(m, n, positions):
    """Union-Find - O(k * alpha(k)) time"""
    if not positions:
        return []
    
    directions = [(0, 1), (0, -1), (1, 0), (-1, 0)]
    uf = UnionFind(m * n)
    grid = [[0] * n for _ in range(m)]
    result = []
    count = 0
    
    for row, col in positions:
        if grid[row][col] == 1:
            result.append(count)
            continue
        
        grid[row][col] = 1
        count += 1
        current = row * n + col
        
        for dr, dc in directions:
            nr, nc = row + dr, col + dc
            if 0 <= nr < m and 0 <= nc < n and grid[nr][nc] == 1:
                neighbor = nr * n + nc
                if uf.find(current) != uf.find(neighbor):
                    uf.union(current, neighbor)
                    count -= 1
        
        result.append(count)
    
    return result


def make_connected(n, connections):
    """Union-Find - O(V + E * alpha(V)) time"""
    if len(connections) < n - 1:
        return -1  # Not enough cables
    
    uf = UnionFind(n)
    
    for u, v in connections:
        uf.union(u, v)
    
    # Count connected components
    roots = set(uf.find(i) for i in range(n))
    components = len(roots)
    
    # Need (components - 1) cables to connect all components
    return components - 1


def smallest_string(s, k):
    """This is a complex problem - simplified version"""
    # This is a placeholder for a more complex solution
    # The full solution would involve greedy approaches
    return s


def max_num_edges_to_remove(n, edges):
    """Union-Find for both colors - O(E * alpha(V)) time"""
    uf_red = UnionFind(n)
    uf_blue = UnionFind(n)
    edges_needed_red = n - 1
    edges_needed_blue = n - 1
    
    for a, b, color in edges:
        if color == 0:  # Red edge
            if uf_red.connected(a, b):
                edges_needed_red -= 1
            else:
                uf_red.union(a, b)
        else:  # Blue edge
            if uf_blue.connected(a, b):
                edges_needed_blue -= 1
            else:
                uf_blue.union(a, b)
    
    return edges_needed_red + edges_needed_blue
'''


def test_all_solutions():
    print("\n" + "=" * 80)
    print("TESTING (requires you to uncomment solutions above)")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 80)
    print("Union-Find (Disjoint Set Union) Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)