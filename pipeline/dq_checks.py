"""
dq_checks.py — Data Quality Validation Layer
Superstore Sales Pipeline
"""
import pandas as pd
import logging
import os
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DQ_RULES = {
    "null_threshold_pct": 5.0,
    "min_rows": 100,
    "non_negative_cols": ["sales", "quantity"],
}


def _to_float(val):
    """Safely convert to float; return None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def check_file_exists(path: str) -> dict:
    """Check that the source file exists."""
    passed = os.path.exists(path)
    return {
        "check": "file_exists",
        "passed": passed,
        "detail": f"File {'found' if passed else 'NOT FOUND'}: {path}",
    }


def check_schema(df: pd.DataFrame, required_cols: list) -> dict:
    """Verify required columns exist."""
    missing = [c for c in required_cols if c not in df.columns]
    passed = len(missing) == 0
    return {
        "check": "schema_match",
        "passed": passed,
        "detail": f"Missing columns: {missing}" if missing else "All required columns present",
    }


def check_row_count(df: pd.DataFrame, min_rows: int = 100) -> dict:
    """Ensure row count is above minimum threshold."""
    passed = len(df) >= min_rows
    return {
        "check": "row_count",
        "passed": passed,
        "detail": f"Row count: {len(df)} (min required: {min_rows})",
    }


def check_null_percentage(df: pd.DataFrame, threshold_pct: float = 5.0) -> dict:
    """Check null % per column is below threshold."""
    results = {}
    all_pass = True
    for col in df.columns:
        null_pct = round(df[col].isnull().mean() * 100, 2)
        col_pass = null_pct < threshold_pct
        results[col] = {"null_pct": null_pct, "passed": col_pass}
        if not col_pass:
            all_pass = False

    failing = [c for c, r in results.items() if not r["passed"]]
    return {
        "check": "null_percentage",
        "passed": all_pass,
        "detail": f"Columns exceeding {threshold_pct}% null: {failing}" if failing else "All columns within null threshold",
        "per_column": results,
    }


def check_duplicates(df: pd.DataFrame, key_cols: list) -> dict:
    """Check for duplicate rows on key columns."""
    available = [c for c in key_cols if c in df.columns]
    if not available:
        return {"check": "duplicates", "passed": True, "detail": "No key columns to check"}
    dup_count = df.duplicated(subset=available).sum()
    passed = dup_count == 0
    return {
        "check": "duplicates",
        "passed": passed,
        "detail": f"{dup_count} duplicate rows found on {available}",
    }


def check_non_negative(df: pd.DataFrame, cols: list) -> dict:
    """Ensure specified numeric columns have no negative values."""
    results = {}
    all_pass = True
    for col in cols:
        if col not in df.columns:
            continue
        numeric_series = pd.to_numeric(df[col], errors="coerce")
        neg_count = int((numeric_series < 0).sum())
        col_pass = neg_count == 0
        results[col] = {"negative_count": neg_count, "passed": col_pass}
        if not col_pass:
            all_pass = False

    return {
        "check": "non_negative_values",
        "passed": all_pass,
        "detail": f"Negative value violations: {results}" if not all_pass else "All numeric constraints satisfied",
        "per_column": results,
    }


def run_dq_checks(csv_path: str, output_path: str = None) -> dict:
    """Run all DQ checks and return scorecard."""
    required_cols = [
        "order_id", "order_date", "ship_date", "customer_id",
        "region", "product_id", "sales", "quantity", "discount", "profit"
    ]

    checks = []

    # File exists
    checks.append(check_file_exists(csv_path))

    if not os.path.exists(csv_path):
        scorecard = _build_scorecard(checks)
        if output_path:
            _save(scorecard, output_path)
        return scorecard

    df = pd.read_csv(csv_path)

    # Schema
    checks.append(check_schema(df, required_cols))

    # Row count
    checks.append(check_row_count(df, DQ_RULES["min_rows"]))

    # Null %
    checks.append(check_null_percentage(df, DQ_RULES["null_threshold_pct"]))

    # Duplicates
    checks.append(check_duplicates(df, ["order_id", "product_id"]))

    # Non-negative
    checks.append(check_non_negative(df, DQ_RULES["non_negative_cols"]))

    scorecard = _build_scorecard(checks)

    if output_path:
        _save(scorecard, output_path)

    return scorecard


def _build_scorecard(checks: list) -> dict:
    """Compile scorecard from check results."""
    total = len(checks)
    passed = sum(1 for c in checks if c["passed"])
    score_pct = round((passed / total) * 100, 1) if total > 0 else 0.0

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "total_checks": total,
        "passed_checks": passed,
        "failed_checks": total - passed,
        "dq_score_pct": score_pct,
        "status": "PASS" if score_pct >= 80 else "FAIL",
        "checks": checks,
    }


def _save(scorecard: dict, path: str):
    """Save scorecard to JSON."""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    def _convert(obj):
        if isinstance(obj, (bool,)):
            return bool(obj)
        if hasattr(obj, "item"):
            return obj.item()
        return obj

    with open(path, "w") as f:
        json.dump(scorecard, f, indent=2, default=_convert)
    logger.info("DQ scorecard saved: %s (score: %s%%)", path, scorecard["dq_score_pct"])


if __name__ == "__main__":
    result = run_dq_checks("data/superstore.csv", "data/dq_scorecard.json")
    print(f"DQ Score: {result['dq_score_pct']}% — Status: {result['status']}")
    for c in result["checks"]:
        status = "PASS" if c["passed"] else "FAIL"
        print(f"  [{status}] {c['check']}: {c['detail']}")
