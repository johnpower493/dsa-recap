"""
Stacks - Practice Exercises (NeetCode 75 style)

Try to solve the problems on your own before looking at the solutions.
Solutions are included below (commented out).
"""

# =============================================================================
# EASY
# =============================================================================

# Problem 1: Valid Parentheses

def is_valid(s):
    """Return True if s has valid brackets."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 2: Min Stack
# Design a stack that supports push, pop, top, and retrieving the minimum element in O(1).

class MinStack:
    def __init__(self):
        # YOUR SOLUTION HERE
        pass

    def push(self, val):
        # YOUR SOLUTION HERE
        pass

    def pop(self):
        # YOUR SOLUTION HERE
        pass

    def top(self):
        # YOUR SOLUTION HERE
        pass

    def getMin(self):
        # YOUR SOLUTION HERE
        pass


# Problem 3: Evaluate Reverse Polish Notation

def evalRPN(tokens):
    """Evaluate RPN expression and return integer result."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Daily Temperatures

def dailyTemperatures(temperatures):
    """Return days to wait for a warmer temperature for each day."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# HARD
# =============================================================================

# Problem 5: Largest Rectangle in Histogram

def largestRectangleArea(heights):
    """Return largest rectangle area in histogram."""
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

def is_valid(s):
    pairs = {')': '(', ']': '[', '}': '{'}
    stack = []
    for ch in s:
        if ch in pairs:
            if not stack or stack[-1] != pairs[ch]:
                return False
            stack.pop()
        else:
            stack.append(ch)
    return not stack


class MinStack:
    def __init__(self):
        self.stack = []
        self.mins = []

    def push(self, val):
        self.stack.append(val)
        if not self.mins:
            self.mins.append(val)
        else:
            self.mins.append(min(val, self.mins[-1]))

    def pop(self):
        self.mins.pop()
        return self.stack.pop()

    def top(self):
        return self.stack[-1]

    def getMin(self):
        return self.mins[-1]


def evalRPN(tokens):
    stack = []
    ops = {"+", "-", "*", "/"}

    for t in tokens:
        if t not in ops:
            stack.append(int(t))
            continue
        b = stack.pop()
        a = stack.pop()
        if t == "+":
            stack.append(a + b)
        elif t == "-":
            stack.append(a - b)
        elif t == "*":
            stack.append(a * b)
        else:
            stack.append(int(a / b))

    return stack[-1]


def dailyTemperatures(temperatures):
    res = [0] * len(temperatures)
    stack = []

    for i, t in enumerate(temperatures):
        while stack and temperatures[stack[-1]] < t:
            j = stack.pop()
            res[j] = i - j
        stack.append(i)

    return res


def largestRectangleArea(heights):
    max_area = 0
    stack = []  # (start_index, height)

    for i, h in enumerate(heights):
        start = i
        while stack and stack[-1][1] > h:
            idx, height = stack.pop()
            max_area = max(max_area, height * (i - idx))
            start = idx
        stack.append((start, h))

    n = len(heights)
    for idx, height in stack:
        max_area = max(max_area, height * (n - idx))

    return max_area
"""


if __name__ == "__main__":
    print("=" * 80)
    print("Stacks Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)
