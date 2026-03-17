"""
Retail Sales Pipeline — Sales Forecasting Dashboard
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates

logger = get_logger("retail_sales_pipeline")

REQUIRED_COLUMNS = ["Store", "Date", "Weekly_Sales"]
BRONZE_PATH = "data/bronze/retail_sales.csv"
SILVER_PATH = "data/silver/retail_sales_clean.csv"
GOLD_PATH = "data/gold/retail_sales_aggregated.csv"


def ingest(path: str) -> pd.DataFrame:
    logger.info(f"[BRONZE] Ingesting from {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[SILVER] Starting transformations")

    dq_report = run_all_checks(df, REQUIRED_COLUMNS)
    logger.info(f"DQ Status: {dq_report['overall_status']}")

    df = remove_duplicates(df)
    df = fill_nulls(df, strategy="median")

    # Parse date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df["Week"] = df["Date"].dt.isocalendar().week.astype(int)
        df["Quarter"] = df["Date"].dt.quarter
        logger.info("Extracted Year, Month, Week, Quarter from Date")

    # IsHoliday as int
    if "IsHoliday" in df.columns:
        df["IsHoliday"] = df["IsHoliday"].astype(int)

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Aggregating for forecasting dashboard")

    sales_col = "Weekly_Sales" if "Weekly_Sales" in df.columns else "Sales"

    # Monthly sales by store
    monthly = df.groupby(["Store", "Year", "Month"])[sales_col].sum().reset_index()
    monthly.columns = ["Store", "Year", "Month", "Monthly_Sales"]

    # Weekly sales trend
    weekly = df.groupby(["Year", "Week"])[sales_col].sum().reset_index()
    weekly.columns = ["Year", "Week", "Weekly_Total"]

    # Holiday vs non-holiday avg
    if "IsHoliday" in df.columns:
        holiday_impact = df.groupby("IsHoliday")[sales_col].mean().reset_index()
        holiday_impact.columns = ["IsHoliday", "Avg_Sales"]
        os.makedirs("data/gold", exist_ok=True)
        holiday_impact.to_csv("data/gold/holiday_sales_impact.csv", index=False)
        logger.info("Saved holiday impact to data/gold/holiday_sales_impact.csv")

    # Save weekly trend
    weekly.to_csv("data/gold/retail_weekly_trend.csv", index=False)
    logger.info("Saved weekly trend to data/gold/retail_weekly_trend.csv")

    logger.info(f"[GOLD] Monthly aggregated shape: {monthly.shape}")
    return monthly


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("RETAIL SALES PIPELINE STARTED")
    logger.info("=" * 50)

    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)

    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)

    logger.info("RETAIL SALES PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
