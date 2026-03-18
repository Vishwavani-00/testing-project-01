"""
Step 1 — Ingestion Layer
Validates source CSV and loads it into stg_superstore.
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, execute_sql_file, get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = {
    "order id", "order date", "ship date", "customer id",
    "region", "product id", "category", "sub-category",
    "product name", "customer name", "sales", "quantity",
    "discount", "profit"
}


def check_file(filepath: str) -> bool:
    """Validate source CSV exists and has required schema."""
    logger.info(f"Checking file: {filepath}")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Source file not found: {filepath}")
    df = pd.read_csv(filepath, nrows=5)
    cols = {c.lower().strip() for c in df.columns}
    missing = REQUIRED_COLUMNS - cols
    if missing:
        raise ValueError(f"Schema mismatch — missing columns: {missing}")
    logger.info("File check PASSED.")
    return True


def load_to_staging(filepath: str, conn) -> int:
    """Truncate staging table and bulk-load fresh CSV data."""
    logger.info(f"Loading {filepath} into stg_superstore ...")
    df = pd.read_csv(filepath)
    df.columns = [c.lower().strip().replace(" ", "_").replace("-", "_") for c in df.columns]

    for col in ["order_date", "ship_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["discount"] = df["discount"].fillna(0.0)

    staging_cols = [
        "order_id", "order_date", "ship_date", "customer_id", "customer_name",
        "region", "product_id", "product_name", "category", "sub_category",
        "sales", "quantity", "discount", "profit"
    ]
    staging_cols = [c for c in staging_cols if c in df.columns]
    df = df[staging_cols]

    # Ensure table exists, then truncate
    sql_dir = os.path.join(os.path.dirname(__file__), "..", "sql")
    execute_sql_file(conn, os.path.join(sql_dir, "01_create_staging.sql"))
    execute_sql(conn, "TRUNCATE TABLE stg_superstore;")

    with conn.cursor() as cur:
        cols_str = ", ".join(staging_cols)
        placeholders = ", ".join(["%s"] * len(staging_cols))
        insert_sql = f"INSERT INTO stg_superstore ({cols_str}) VALUES ({placeholders})"
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, rows)
    conn.commit()

    logger.info(f"Loaded {len(df)} rows into stg_superstore.")
    return len(df)


def run(config_path: str = None):
    config = load_config(config_path)
    filepath = os.path.join(config["pipeline"]["data_dir"], config["pipeline"]["source_file"])
    check_file(filepath)
    conn = get_connection(config)
    try:
        load_to_staging(filepath, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
