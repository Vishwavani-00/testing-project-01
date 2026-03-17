import pandas as pd
from utils.logger import get_logger

logger = get_logger("data_quality")


def check_null_threshold(df: pd.DataFrame, threshold: float = 0.10) -> dict:
    """Check if null percentage per column exceeds threshold."""
    results = {}
    for col in df.columns:
        null_pct = df[col].isnull().mean()
        status = "PASS" if null_pct <= threshold else "FAIL"
        results[col] = {"null_pct": round(null_pct, 4), "status": status}
        if status == "FAIL":
            logger.warning(f"Column '{col}' has {null_pct:.1%} nulls — exceeds {threshold:.0%} threshold")
    return results


def check_duplicates(df: pd.DataFrame) -> dict:
    """Check for duplicate rows."""
    dup_count = df.duplicated().sum()
    status = "PASS" if dup_count == 0 else "WARNING"
    logger.info(f"Duplicate rows found: {dup_count}")
    return {"duplicate_count": dup_count, "status": status}


def validate_schema(df: pd.DataFrame, required_columns: list) -> dict:
    """Validate that required columns exist in the dataframe."""
    missing = [col for col in required_columns if col not in df.columns]
    status = "PASS" if not missing else "FAIL"
    if missing:
        logger.error(f"Missing required columns: {missing}")
    return {"missing_columns": missing, "status": status}


def run_all_checks(df: pd.DataFrame, required_columns: list, threshold: float = 0.10) -> dict:
    """Run all data quality checks and return a summary report."""
    logger.info(f"Running DQ checks on dataframe with shape {df.shape}")
    report = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "schema_check": validate_schema(df, required_columns),
        "null_check": check_null_threshold(df, threshold),
        "duplicate_check": check_duplicates(df),
    }
    overall = all([
        report["schema_check"]["status"] == "PASS",
        report["duplicate_check"]["status"] == "PASS",
        all(v["status"] == "PASS" for v in report["null_check"].values())
    ])
    report["overall_status"] = "PASS" if overall else "WARNING"
    logger.info(f"DQ Overall Status: {report['overall_status']}")
    return report
