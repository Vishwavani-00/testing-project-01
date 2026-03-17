"""
Credit Card Fraud Pipeline — Anomaly Detection
Bronze → Silver → Gold
"""

import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, scale_features

logger = get_logger("fraud_pipeline")

REQUIRED_COLUMNS = ["Amount", "Time", "Class"]
BRONZE_PATH = "data/bronze/creditcard.csv"
SILVER_PATH = "data/silver/fraud_clean.csv"
GOLD_PATH = "data/gold/fraud_features.csv"


def ingest(path: str) -> pd.DataFrame:
    logger.info(f"[BRONZE] Ingesting from {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    fraud_rate = df["Class"].mean() if "Class" in df.columns else "N/A"
    logger.info(f"Fraud rate in raw data: {fraud_rate:.4%}" if isinstance(fraud_rate, float) else "N/A")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[SILVER] Starting transformations")

    dq_report = run_all_checks(df, REQUIRED_COLUMNS)
    logger.info(f"DQ Status: {dq_report['overall_status']}")

    df = remove_duplicates(df)
    df = fill_nulls(df, strategy="median")

    # Log-transform Amount to handle skewness
    if "Amount" in df.columns:
        df["Amount_log"] = np.log1p(df["Amount"])
        logger.info("Created log-transformed Amount feature")

    # Normalize Time to hours
    if "Time" in df.columns:
        df["Time_hours"] = df["Time"] / 3600
        logger.info("Converted Time to hours")

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering for anomaly detection")

    # Scale V features (PCA components)
    v_cols = [c for c in df.columns if c.startswith("V")]
    if v_cols:
        df = scale_features(df, v_cols)

    # Scale Amount_log
    if "Amount_log" in df.columns:
        df = scale_features(df, ["Amount_log"])

    # Class balance summary
    if "Class" in df.columns:
        class_dist = df["Class"].value_counts()
        logger.info(f"Class distribution — Legit: {class_dist.get(0, 0)}, Fraud: {class_dist.get(1, 0)}")

        # Separate fraud/legit for downstream use
        fraud_df = df[df["Class"] == 1]
        legit_sample = df[df["Class"] == 0].sample(n=min(len(fraud_df) * 10, len(df[df["Class"] == 0])),
                                                     random_state=42)
        df_balanced = pd.concat([fraud_df, legit_sample]).sample(frac=1, random_state=42).reset_index(drop=True)
        logger.info(f"Balanced dataset: {len(df_balanced)} rows (fraud: {len(fraud_df)}, legit sample: {len(legit_sample)})")

        os.makedirs("data/gold", exist_ok=True)
        df_balanced.to_csv("data/gold/fraud_balanced.csv", index=False)
        logger.info("Saved balanced dataset to data/gold/fraud_balanced.csv")

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("FRAUD PIPELINE STARTED")
    logger.info("=" * 50)

    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)

    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)

    logger.info("FRAUD PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
