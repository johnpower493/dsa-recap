"""
Linked Lists - Core Patterns (NeetCode 75 aligned)

This module provides a small Node class and common linked-list algorithms:
- Reverse linked list
- Merge two sorted lists
- Detect cycle (Floyd)
- Reorder list (L0->Ln->L1->Ln-1...)

Time Complexity: typically O(n)
Space Complexity: O(1) (in-place), except helper conversions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, List


@dataclass
class ListNode:
    val: int
    next: Optional["ListNode"] = None


def from_list(values: Iterable[int]) -> Optional[ListNode]:
    """Build a linked list from python list."""
    head: Optional[ListNode] = None
    tail: Optional[ListNode] = None

    for v in values:
        node = ListNode(v)
        if head is None:
            head = tail = node
        else:
            assert tail is not None
            tail.next = node
            tail = node

    return head


def to_list(head: Optional[ListNode]) -> List[int]:
    """Convert linked list to python list."""
    out: List[int] = []
    cur = head
    while cur:
        out.append(cur.val)
        cur = cur.next
    return out


def reverse_list(head: Optional[ListNode]) -> Optional[ListNode]:
    """Reverse a singly linked list."""
    prev: Optional[ListNode] = None
    cur = head

    while cur:
        nxt = cur.next
        cur.next = prev
        prev = cur
        cur = nxt

    return prev


def merge_two_sorted_lists(l1: Optional[ListNode], l2: Optional[ListNode]) -> Optional[ListNode]:
    """Merge two sorted linked lists and return head of merged list."""
    dummy = ListNode(0)
    tail = dummy

    a, b = l1, l2
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


def has_cycle(head: Optional[ListNode]) -> bool:
    """Detect cycle using Floyd's tortoise and hare."""
    slow = head
    fast = head

    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow is fast:
            return True

    return False


def reorder_list(head: Optional[ListNode]) -> Optional[ListNode]:
    """Reorder list in-place: L0->L1->...->Ln to L0->Ln->L1->Ln-1->...

    Returns head for convenience.

    Steps:
    1) Find middle
    2) Reverse second half
    3) Merge alternating
    """
    if not head or not head.next:
        return head

    # 1) Find middle (slow ends at mid)
    slow: ListNode = head
    fast: ListNode = head
    while fast.next and fast.next.next:
        slow = slow.next  # type: ignore[assignment]
        fast = fast.next.next

    # 2) Reverse second half
    second = slow.next
    slow.next = None
    second = reverse_list(second)

    # 3) Merge alternating
    first = head
    while second:
        tmp1 = first.next
        tmp2 = second.next
        first.next = second
        second.next = tmp1
        first = tmp1 if tmp1 else second
        second = tmp2

    return head


if __name__ == "__main__":
    print("=" * 70)
    print("Linked Lists - Examples")
    print("=" * 70)

    # 1) Reverse list
    head = from_list([1, 2, 3, 4, 5])
    print("\n1. Reverse List")
    print("before:", to_list(head))
    head = reverse_list(head)
    print("after: ", to_list(head))

    # 2) Merge two sorted lists
    print("\n2. Merge Two Sorted Lists")
    l1 = from_list([1, 2, 4])
    l2 = from_list([1, 3, 4])
    merged = merge_two_sorted_lists(l1, l2)
    print("merged:", to_list(merged))

    # 3) Cycle detection
    print("\n3. Detect Cycle")
    cyc = from_list([1, 2, 3])
    assert cyc and cyc.next and cyc.next.next
    cyc.next.next.next = cyc.next  # create cycle
    print("has_cycle:", has_cycle(cyc))

    # 4) Reorder list
    print("\n4. Reorder List")
    head = from_list([1, 2, 3, 4, 5])
    print("before:", to_list(head))
    reorder_list(head)
    print("after: ", to_list(head))
