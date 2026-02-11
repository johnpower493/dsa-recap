"""
Pandas Advanced - Reference Solutions
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
    return orders_df.merge(customers_df, on="customer_id", how="left")


def ex2_segment_metrics(joined_df: pd.DataFrame) -> pd.DataFrame:
    return (
        joined_df.groupby("segment", as_index=False)
        .agg(
            total_sales=("amount", "sum"),
            avg_sales=("amount", "mean"),
            order_count=("order_id", "count"),
        )
        .sort_values("total_sales", ascending=False)
    )


def ex3_category_month_pivot(orders_df: pd.DataFrame) -> pd.DataFrame:
    return orders_df.pivot_table(
        index="month", columns="category", values="amount", aggfunc="sum", fill_value=0
    ).reset_index()


def ex4_melt_back_to_long(pivot_df: pd.DataFrame) -> pd.DataFrame:
    return pivot_df.melt(id_vars=["month"], var_name="category", value_name="total_amount")


if __name__ == "__main__":
    joined = ex1_merge(orders, customers)
    print("\nJoined:")
    print(joined)

    print("\nSegment metrics:")
    print(ex2_segment_metrics(joined))

    pivot = ex3_category_month_pivot(orders)
    print("\nPivot:")
    print(pivot)

    print("\nLong format:")
    print(ex4_melt_back_to_long(pivot))
