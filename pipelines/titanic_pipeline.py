"""
Titanic Pipeline — Survival Prediction & Feature Engineering
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, one_hot_encode

logger = get_logger("titanic_pipeline")

REQUIRED_COLUMNS = ["Age", "Sex", "Fare", "Pclass", "Survived"]
BRONZE_PATH = "data/bronze/titanic.csv"
SILVER_PATH = "data/silver/titanic_clean.csv"
GOLD_PATH = "data/gold/titanic_features.csv"


def ingest(path: str) -> pd.DataFrame:
    """Bronze: Load raw Titanic CSV."""
    logger.info(f"[BRONZE] Ingesting from {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Silver: Clean and transform Titanic data."""
    logger.info("[SILVER] Starting transformations")

    # DQ checks
    dq_report = run_all_checks(df, REQUIRED_COLUMNS)
    logger.info(f"DQ Status: {dq_report['overall_status']}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Fill nulls
    df = fill_nulls(df, strategy="median")

    # Drop low-value columns
    drop_cols = ["Cabin", "Ticket", "Name", "PassengerId"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Label encode Sex
    df = label_encode(df, ["Sex"])

    # One-hot encode Embarked
    df = one_hot_encode(df, ["Embarked"])

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Gold: Feature engineering for ML."""
    logger.info("[GOLD] Feature engineering")

    # Family size feature
    if "SibSp" in df.columns and "Parch" in df.columns:
        df["FamilySize"] = df["SibSp"] + df["Parch"] + 1
        df["IsAlone"] = (df["FamilySize"] == 1).astype(int)

    # Fare bins
    if "Fare" in df.columns:
        df["FareBin"] = pd.qcut(df["Fare"], q=4, labels=["Low", "Mid", "High", "VeryHigh"])
        df = label_encode(df, ["FareBin"])

    # Age bins
    if "Age" in df.columns:
        df["AgeBin"] = pd.cut(df["Age"], bins=[0, 12, 18, 35, 60, 100],
                               labels=["Child", "Teen", "Adult", "MidAge", "Senior"])
        df = label_encode(df, ["AgeBin"])

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    """Save dataframe to CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("TITANIC PIPELINE STARTED")
    logger.info("=" * 50)

    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)

    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)

    logger.info("TITANIC PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
