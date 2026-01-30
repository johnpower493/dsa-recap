"""
Linked Lists - Practice Exercises (NeetCode 75 style)

Try to solve first. Solutions are included below (commented out).
"""

# =============================================================================
# Node definition (keep simple and consistent)
# =============================================================================

class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


# =============================================================================
# EASY
# =============================================================================

# Problem 1: Reverse Linked List

def reverseList(head):
    """Return the reversed linked list head."""
    # YOUR SOLUTION HERE
    pass


# =============================================================================
# MEDIUM
# =============================================================================

# Problem 2: Merge Two Sorted Lists

def mergeTwoLists(list1, list2):
    """Merge two sorted linked lists."""
    # YOUR SOLUTION HERE
    pass


# Problem 3: Linked List Cycle

def hasCycle(head):
    """Return True if the linked list has a cycle."""
    # YOUR SOLUTION HERE
    pass


# Problem 4: Reorder List
# Reorder to L0->Ln->L1->Ln-1->...

def reorderList(head):
    """Reorder list in-place. Return head for convenience."""
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

def reverseList(head):
    prev = None
    cur = head
    while cur:
        nxt = cur.next
        cur.next = prev
        prev = cur
        cur = nxt
    return prev


def mergeTwoLists(list1, list2):
    dummy = ListNode(0)
    tail = dummy

    a, b = list1, list2
    while a and b:
        if a.val <= b.val:
            tail.next = a
            a = a.next
        else:
            tail.next = b
            b = b.next
        tail = tail.next

    tail.next = a if a else b
    return dummy.next


def hasCycle(head):
    slow = head
    fast = head
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow is fast:
            return True
    return False


def reorderList(head):
    if not head or not head.next:
        return head

    # Find middle
    slow = head
    fast = head
    while fast.next and fast.next.next:
        slow = slow.next
        fast = fast.next.next

    # Reverse second half
    second = slow.next
    slow.next = None

    prev = None
    cur = second
    while cur:
        nxt = cur.next
        cur.next = prev
        prev = cur
        cur = nxt
    second = prev

    # Merge alternating
    first = head
    while second:
        tmp1 = first.next
        tmp2 = second.next
        first.next = second
        second.next = tmp1
        first = tmp1 if tmp1 else second
        second = tmp2

    return head
"""


if __name__ == "__main__":
    print("=" * 80)
    print("Linked Lists Practice Exercises")
    print("=" * 80)
    print("\nTo see solutions, uncomment the solutions section in the code.")
    print("=" * 80)
