"""
Pandas - Reference Solutions
"""

import pandas as pd


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


def exercise_1(df: pd.DataFrame):
    print(df.head(3))
    print(df.dtypes)
    print(df.describe())


def exercise_2(df: pd.DataFrame):
    books = df[df["category"] == "Books"]
    gt_50 = df[df["amount"] > 50]
    home_or_200 = df[(df["category"] == "Home") | (df["amount"] > 200)]
    return books, gt_50, home_or_200


def exercise_3(df: pd.DataFrame):
    out = df.copy()
    out["amount_with_tax"] = out["amount"] * 1.10
    out["is_large_order"] = out["amount"] >= 100
    return out


def exercise_4(df: pd.DataFrame):
    return (
        df.groupby("category", as_index=False)
        .agg(
            total_sales=("amount", "sum"),
            avg_sales=("amount", "mean"),
            order_count=("order_id", "count"),
        )
    )


def exercise_5(df: pd.DataFrame):
    return df.nlargest(2, "amount")[["order_id", "customer_id", "amount"]]


def exercise_6(df: pd.DataFrame):
    out = df.copy()
    out["order_date"] = pd.to_datetime(out["order_date"])
    out["order_weekday"] = out["order_date"].dt.day_name()
    return out


if __name__ == "__main__":
    print("\nExercise 1")
    exercise_1(orders)

    print("\nExercise 2")
    books, gt_50, home_or_200 = exercise_2(orders)
    print("Books:\n", books)
    print("Amount > 50:\n", gt_50)
    print("Home OR amount > 200:\n", home_or_200)

    print("\nExercise 3")
    print(exercise_3(orders))

    print("\nExercise 4")
    print(exercise_4(orders))

    print("\nExercise 5")
    print(exercise_5(orders))

    print("\nExercise 6")
    print(exercise_6(orders))
