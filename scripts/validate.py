"""
Step 2 — Data Validation Layer
Quality checks on stg_superstore before transformation.
Rules:
  - Row count > 0
  - Null % on critical columns < 5%
  - No duplicate (order_id, product_id) pairs (warning, not fail)
  - sales >= 0
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, get_logger

logger = get_logger(__name__)
MAX_NULL_PCT = 0.05


def check_row_count(conn) -> int:
    rows = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore;", fetch=True)
    count = rows[0]["cnt"]
    if count == 0:
        raise ValueError("Quality FAIL: stg_superstore is empty.")
    logger.info(f"Row count PASS: {count} rows")
    return count


def check_nulls(conn):
    critical = ["order_id", "order_date", "customer_id", "product_id", "sales", "profit"]
    total = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore;", fetch=True)[0]["cnt"]
    for col in critical:
        result = execute_sql(conn, f"SELECT COUNT(*) AS cnt FROM stg_superstore WHERE {col} IS NULL;", fetch=True)
        pct = result[0]["cnt"] / total if total > 0 else 0
        if pct > MAX_NULL_PCT:
            raise ValueError(f"Quality FAIL: '{col}' has {pct:.1%} nulls (max {MAX_NULL_PCT:.0%})")
        logger.info(f"Null check PASS [{col}]: {pct:.2%}")


def check_duplicates(conn):
    result = execute_sql(conn, """
        SELECT COUNT(*) AS dup_count FROM (
            SELECT order_id, product_id FROM stg_superstore
            GROUP BY order_id, product_id HAVING COUNT(*) > 1
        ) d;
    """, fetch=True)
    dup_count = result[0]["dup_count"]
    if dup_count > 0:
        logger.warning(f"Duplicate WARNING: {dup_count} pairs — will deduplicate in transform.")
    else:
        logger.info("Duplicate check PASS.")
    return dup_count


def check_sales_constraint(conn):
    result = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore WHERE sales < 0;", fetch=True)
    if result[0]["cnt"] > 0:
        raise ValueError(f"Quality FAIL: {result[0]['cnt']} rows with sales < 0.")
    logger.info("Sales >= 0 PASS.")


def run(config_path: str = None):
    config = load_config(config_path)
    conn = get_connection(config)
    try:
        logger.info("=== Starting Data Validation ===")
        check_row_count(conn)
        check_nulls(conn)
        check_duplicates(conn)
        check_sales_constraint(conn)
        logger.info("=== Data Validation PASSED ===")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
