import pandas as pd

def clean_sales_data(file_path: str):
    df = pd.read_csv(file_path)

    df.columns = [c.strip().lower() for c in df.columns]

    before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = before - len(df)

    rows_before = len(df)
    df = df.dropna(subset=["sale_id", "store_id", "product_id", "customer_id", "qty", "price"])
    rows_removed = rows_before - len(df)

    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["total"] = df["qty"] * df["price"]

    return df, duplicates_removed, rows_removed
