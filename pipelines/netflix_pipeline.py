"""
Netflix Movies Pipeline — Content Analytics Dashboard
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, split_multi_value_column

logger = get_logger("netflix_pipeline")

REQUIRED_COLUMNS = ["title", "type", "listed_in", "release_year", "country"]
BRONZE_PATH = "data/bronze/netflix_titles.csv"
SILVER_PATH = "data/silver/netflix_clean.csv"
GOLD_PATH = "data/gold/netflix_aggregated.csv"


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

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Parse date_added to year/month
    if "date_added" in df.columns:
        df["date_added"] = pd.to_datetime(df["date_added"].str.strip(), errors="coerce")
        df["added_year"] = df["date_added"].dt.year
        df["added_month"] = df["date_added"].dt.month

    # Extract primary country (first value)
    if "country" in df.columns:
        df["primary_country"] = df["country"].str.split(",").str[0].str.strip()

    # Duration parsing
    if "duration" in df.columns:
        df["duration_value"] = df["duration"].str.extract(r"(\d+)").astype(float)
        df["duration_unit"] = df["duration"].str.extract(r"([a-zA-Z]+)")

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Aggregating for dashboard")

    # Explode genres
    genre_df = None
    if "listed_in" in df.columns:
        genre_df = split_multi_value_column(df[["show_id", "type", "release_year", "listed_in"]].copy(),
                                             "listed_in", ",")
        genre_df = genre_df.rename(columns={"listed_in": "genre"})

    # Content by type and year
    type_year = df.groupby(["type", "release_year"]).size().reset_index(name="count")

    # Top countries
    country_counts = df["primary_country"].value_counts().reset_index()
    country_counts.columns = ["country", "title_count"]

    # Save genre breakdown separately
    if genre_df is not None:
        os.makedirs("data/gold", exist_ok=True)
        genre_df.to_csv("data/gold/netflix_genres.csv", index=False)
        logger.info("Saved genre breakdown to data/gold/netflix_genres.csv")

    # Main gold = content by type + year
    logger.info(f"[GOLD] Final shape: {type_year.shape}")
    return type_year


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("NETFLIX PIPELINE STARTED")
    logger.info("=" * 50)

    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)

    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)

    logger.info("NETFLIX PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
