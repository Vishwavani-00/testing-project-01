"""
transform.py — ETL Transformation Layer
Superstore Sales Pipeline
"""
import pandas as pd
import numpy as np
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _to_float(val):
    """Safely convert value to float, return None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def load_raw(csv_path: str) -> pd.DataFrame:
    """Load raw CSV from staging path."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d rows x %d cols from %s", len(df), len(df.columns), csv_path)
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning rules per PRD §6.3."""
    original_len = len(df)

    # Remove duplicates on (order_id, product_id)
    df = df.drop_duplicates(subset=["order_id", "product_id"])
    logger.info("Dedup: %d → %d rows", original_len, len(df))

    # Drop rows where sales or profit is null
    df = df.dropna(subset=["sales", "profit"])

    # Discount: fill nulls with 0
    df["discount"] = df["discount"].fillna(0)

    # Coerce numeric columns safely
    for col in ["sales", "profit", "discount", "quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where numeric coercion failed on critical fields
    df = df.dropna(subset=["sales", "profit"])

    # Ensure sales >= 0 (flag negatives but keep — log warning)
    neg_sales = (df["sales"] < 0).sum()
    if neg_sales > 0:
        logger.warning("%d rows with negative sales found", neg_sales)

    logger.info("After cleaning: %d rows", len(df))
    return df


def derive_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns per PRD §6.3."""
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    df["order_month"] = df["order_date"].dt.month
    df["order_year"] = df["order_date"].dt.year
    df["profit_margin"] = np.where(
        df["sales"] != 0,
        (df["profit"] / df["sales"]).round(4),
        0.0
    )
    logger.info("Derived columns added: order_month, order_year, profit_margin")
    return df


def build_fact_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Build fact_sales table."""
    cols = ["order_id", "customer_id", "product_id", "order_date", "sales",
            "quantity", "profit", "discount", "order_month", "order_year", "profit_margin"]
    available = [c for c in cols if c in df.columns]
    fact = df[available].copy()
    fact = fact.reset_index(drop=True)
    fact.index.name = "fact_id"
    logger.info("fact_sales: %d rows", len(fact))
    return fact


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """Build dim_customer dimension table."""
    cols = ["customer_id", "customer_name", "segment"]
    available = [c for c in cols if c in df.columns]
    dim = df[available].drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
    logger.info("dim_customer: %d rows", len(dim))
    return dim


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Build dim_product dimension table."""
    cols = ["product_id", "product_name", "category", "sub_category"]
    available = [c for c in cols if c in df.columns]
    dim = df[available].drop_duplicates(subset=["product_id"]).reset_index(drop=True)
    logger.info("dim_product: %d rows", len(dim))
    return dim


def build_dim_region(df: pd.DataFrame) -> pd.DataFrame:
    """Build dim_region dimension table."""
    cols = ["region", "state", "city", "country"]
    available = [c for c in cols if c in df.columns]
    dim = df[available].drop_duplicates(subset=["region"]).reset_index(drop=True)
    logger.info("dim_region: %d rows", len(dim))
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Build dim_date dimension table from all order dates."""
    dates = df["order_date"].dropna().unique()
    dim = pd.DataFrame({"order_date": pd.to_datetime(dates)})
    dim["day"] = dim["order_date"].dt.day
    dim["month"] = dim["order_date"].dt.month
    dim["year"] = dim["order_date"].dt.year
    dim["quarter"] = dim["order_date"].dt.quarter
    dim["day_of_week"] = dim["order_date"].dt.day_name()
    dim = dim.sort_values("order_date").reset_index(drop=True)
    logger.info("dim_date: %d rows", len(dim))
    return dim


def build_agg_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build aggregated sales summary table per PRD §6.4."""
    group_cols = []
    for c in ["order_date", "region", "category"]:
        if c in df.columns:
            group_cols.append(c)

    agg = df.groupby(group_cols).agg(
        total_sales=("sales", "sum"),
        total_profit=("profit", "sum"),
        total_orders=("order_id", "count"),
        avg_discount=("discount", "mean")
    ).reset_index()

    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    logger.info("agg_sales_summary: %d rows", len(agg))
    return agg


def run_transform(csv_path: str, output_dir: str) -> dict:
    """Full transform pipeline — returns dict of DataFrames."""
    os.makedirs(output_dir, exist_ok=True)

    df = load_raw(csv_path)
    df = clean(df)
    df = derive_columns(df)

    tables = {
        "fact_sales": build_fact_sales(df),
        "dim_customer": build_dim_customer(df),
        "dim_product": build_dim_product(df),
        "dim_region": build_dim_region(df),
        "dim_date": build_dim_date(df),
        "agg_sales_summary": build_agg_sales_summary(df),
    }

    for name, tbl in tables.items():
        out_path = os.path.join(output_dir, f"{name}.csv")
        tbl.to_csv(out_path, index=False)
        logger.info("Saved %s → %s", name, out_path)

    return tables


if __name__ == "__main__":
    run_transform("data/superstore.csv", "data/transformed")
