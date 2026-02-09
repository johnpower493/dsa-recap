"""
Pandas - Practice Exercises

Instructions:
- Complete each TODO.
- Use idiomatic pandas where possible.
- Run this file after each section to validate your progress.
"""

import pandas as pd


# Sample data for exercises
orders = pd.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5, 6],
        "customer_id": [101, 102, 101, 103, 104, 102],
        "category": ["Books", "Electronics", "Books", "Home", "Books", "Home"],
        "amount": [25.0, 300.0, 18.5, 75.0, 42.0, 120.0],
        "order_date": [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
            "2026-01-04",
            "2026-01-05",
            "2026-01-06",
        ],
    }
)


# =============================================================================
# EXERCISE 1: BASIC INSPECTION
# =============================================================================
def exercise_1(df: pd.DataFrame):
    """
    TODO:
      1) Print the first 3 rows
      2) Print dtypes
      3) Print basic summary stats for numeric columns
    """
    pass


# =============================================================================
# EXERCISE 2: FILTERING
# =============================================================================
def exercise_2(df: pd.DataFrame):
    """
    TODO:
      1) Return rows where category == "Books"
      2) Return rows where amount > 50
      3) Return rows where category is "Home" OR amount > 200
    """
    pass


# =============================================================================
# EXERCISE 3: NEW COLUMNS
# =============================================================================
def exercise_3(df: pd.DataFrame):
    """
    TODO:
      1) Add column 'amount_with_tax' = amount * 1.10
      2) Add column 'is_large_order' where amount >= 100 (True/False)
      3) Return updated DataFrame
    """
    pass


# =============================================================================
# EXERCISE 4: GROUPBY + AGGREGATION
# =============================================================================
def exercise_4(df: pd.DataFrame):
    """
    TODO:
      Group by category and compute:
      - total_sales (sum of amount)
      - avg_sales (mean of amount)
      - order_count (count of order_id)

      Return a DataFrame with category as a column (not index).
    """
    pass


# =============================================================================
# EXERCISE 5: SORTING + TOP N
# =============================================================================
def exercise_5(df: pd.DataFrame):
    """
    TODO:
      1) Find top 2 highest-value orders by amount
      2) Return only columns: order_id, customer_id, amount
    """
    pass


# =============================================================================
# EXERCISE 6: DATES
# =============================================================================
def exercise_6(df: pd.DataFrame):
    """
    TODO:
      1) Convert order_date to datetime
      2) Add column 'order_weekday' with day name (e.g., Monday)
      3) Return updated DataFrame
    """
    pass


if __name__ == "__main__":
    print("Pandas exercises ready. Complete TODO sections and run this file to test.")
