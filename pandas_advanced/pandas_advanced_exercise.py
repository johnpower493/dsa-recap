"""
Pandas Advanced - Practice Exercises

Focus:
- merge/join
- reshape (melt/pivot_table)
- groupby with multiple aggregations
"""

import pandas as pd


customers = pd.DataFrame(
    {
        "customer_id": [101, 102, 103, 104],
        "segment": ["SMB", "Enterprise", "SMB", "Mid-Market"],
    }
)

orders = pd.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5, 6],
        "customer_id": [101, 102, 101, 103, 104, 102],
        "category": ["Books", "Electronics", "Books", "Home", "Books", "Home"],
        "amount": [25.0, 300.0, 18.5, 75.0, 42.0, 120.0],
        "month": ["2026-01", "2026-01", "2026-02", "2026-02", "2026-03", "2026-03"],
    }
)


def ex1_merge(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """TODO: Left-join orders with customers on customer_id."""
    pass


def ex2_segment_metrics(joined_df: pd.DataFrame) -> pd.DataFrame:
    """TODO: Group by segment and return total_sales + avg_sales + order_count."""
    pass


def ex3_category_month_pivot(orders_df: pd.DataFrame) -> pd.DataFrame:
    """TODO: Build pivot table: index=month, columns=category, values=sum(amount)."""
    pass


def ex4_melt_back_to_long(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """TODO: Convert pivot result back to long format with columns: month, category, total_amount."""
    pass


if __name__ == "__main__":
    print("Complete TODOs in pandas_advanced_exercise.py")
