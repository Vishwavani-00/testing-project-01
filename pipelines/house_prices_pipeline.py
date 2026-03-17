"""
House Prices Pipeline — Price Prediction & Regression Modeling
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, remove_outliers_iqr, scale_features

logger = get_logger("house_prices_pipeline")

REQUIRED_COLUMNS = ["LotArea", "OverallQual", "YearBuilt", "SalePrice"]
BRONZE_PATH = "data/bronze/house_prices.csv"
SILVER_PATH = "data/silver/house_prices_clean.csv"
GOLD_PATH = "data/gold/house_prices_features.csv"


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

    # Drop high-null columns
    null_pct = df.isnull().mean()
    high_null_cols = null_pct[null_pct > 0.5].index.tolist()
    df = df.drop(columns=high_null_cols)
    logger.info(f"Dropped high-null columns: {high_null_cols}")

    # Encode categorical columns
    cat_cols = df.select_dtypes(include="object").columns.tolist()
    df = label_encode(df, cat_cols)

    # Remove outliers on SalePrice
    if "SalePrice" in df.columns:
        df = remove_outliers_iqr(df, "SalePrice")

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering")

    # Age of house at time of sale
    if "YearBuilt" in df.columns and "YrSold" in df.columns:
        df["HouseAge"] = df["YrSold"] - df["YearBuilt"]

    # Remodel flag
    if "YearRemodAdd" in df.columns and "YearBuilt" in df.columns:
        df["IsRemodeled"] = (df["YearRemodAdd"] > df["YearBuilt"]).astype(int)

    # Total area
    if "TotalBsmtSF" in df.columns and "GrLivArea" in df.columns:
        df["TotalArea"] = df["TotalBsmtSF"] + df["GrLivArea"]

    # Scale numeric features
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns.tolist()
    exclude = ["SalePrice", "Id"]
    scale_cols = [c for c in num_cols if c not in exclude and c in df.columns]
    if scale_cols:
        df = scale_features(df, scale_cols)

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("HOUSE PRICES PIPELINE STARTED")
    logger.info("=" * 50)

    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)

    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)

    logger.info("HOUSE PRICES PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
