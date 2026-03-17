#!/usr/bin/env python3
"""
Kaggle ETL Pipeline — Real Data (Titanic, House Prices, Netflix)
Usage: python3 pipeline.py [--bronze-path PATH] [--output-path PATH]
"""

import argparse
import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import LabelEncoder

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger("kaggle_pipeline")


# ─────────────────────────────────────────────
# Data Quality Checks
# ─────────────────────────────────────────────
def run_dq_checks(df: pd.DataFrame, name: str) -> dict:
    nulls = df.isnull().sum().to_dict()
    dupes = int(df.duplicated().sum())
    logger.info(f"[{name}] DQ — rows: {len(df)}, dupes: {dupes}, null cols: {[k for k,v in nulls.items() if v > 0]}")
    return {"nulls": nulls, "duplicates": dupes, "row_count": len(df), "status": "PASS"}


# ─────────────────────────────────────────────
# TITANIC PIPELINE
# ─────────────────────────────────────────────
def run_titanic(bronze_path: str, silver_dir: str, gold_dir: str) -> dict:
    logger.info("=" * 50)
    logger.info("TITANIC PIPELINE STARTED")

    # Bronze
    df = pd.read_csv(bronze_path)
    logger.info(f"[BRONZE] Loaded {df.shape[0]} rows x {df.shape[1]} cols")
    dq = run_dq_checks(df, "TITANIC")

    # Silver — fill nulls with 0, normalize, encode
    df = df.fillna(0)
    df.columns = df.columns.str.lower().str.strip()
    df['sex'] = df['sex'].map({'male': 1, 'female': 0}).fillna(0).astype(int)
    os.makedirs(silver_dir, exist_ok=True)
    df.to_csv(f"{silver_dir}/titanic_clean.csv", index=False)
    logger.info(f"[SILVER] Saved {df.shape[0]} rows → {silver_dir}/titanic_clean.csv")

    # Gold — feature engineering
    df['familysize'] = df['sibsp'] + df['parch'] + 1
    df['isalone'] = (df['familysize'] == 1).astype(int)
    df['farebin'] = pd.qcut(df['fare'], q=4, labels=[0, 1, 2, 3]).astype(int)
    os.makedirs(gold_dir, exist_ok=True)
    df.to_csv(f"{gold_dir}/titanic_features.csv", index=False)
    logger.info(f"[GOLD] Saved {df.shape[0]} rows → {gold_dir}/titanic_features.csv")
    logger.info("TITANIC PIPELINE COMPLETED ✓")

    return {"dataset": "titanic", "rows": len(df), "dq": dq}


# ─────────────────────────────────────────────
# HOUSE PRICES PIPELINE
# ─────────────────────────────────────────────
def run_house_prices(bronze_path: str, silver_dir: str, gold_dir: str) -> dict:
    logger.info("=" * 50)
    logger.info("HOUSE PRICES PIPELINE STARTED")

    # Bronze
    df = pd.read_csv(bronze_path)
    logger.info(f"[BRONZE] Loaded {df.shape[0]} rows x {df.shape[1]} cols")
    dq = run_dq_checks(df, "HOUSE_PRICES")

    # Silver — fill nulls with 0, encode
    df = df.fillna(0)
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    le = LabelEncoder()
    df['ocean_proximity'] = le.fit_transform(df['ocean_proximity'].astype(str))
    os.makedirs(silver_dir, exist_ok=True)
    df.to_csv(f"{silver_dir}/house_prices_clean.csv", index=False)
    logger.info(f"[SILVER] Saved {df.shape[0]} rows → {silver_dir}/house_prices_clean.csv")

    # Gold — feature engineering
    df['rooms_per_household'] = (df['total_rooms'] / df['households'].replace(0, np.nan)).fillna(0).round(2)
    df['bedrooms_per_room'] = (df['total_bedrooms'] / df['total_rooms'].replace(0, np.nan)).fillna(0).round(4)
    df['population_per_household'] = (df['population'] / df['households'].replace(0, np.nan)).fillna(0).round(2)
    os.makedirs(gold_dir, exist_ok=True)
    df.to_csv(f"{gold_dir}/house_prices_features.csv", index=False)
    logger.info(f"[GOLD] Saved {df.shape[0]} rows → {gold_dir}/house_prices_features.csv")
    logger.info("HOUSE PRICES PIPELINE COMPLETED ✓")

    return {"dataset": "house_prices", "rows": len(df), "dq": dq}


# ─────────────────────────────────────────────
# NETFLIX PIPELINE
# ─────────────────────────────────────────────
def run_netflix(bronze_path: str, silver_dir: str, gold_dir: str) -> dict:
    logger.info("=" * 50)
    logger.info("NETFLIX PIPELINE STARTED")

    # Bronze
    df = pd.read_csv(bronze_path)
    logger.info(f"[BRONZE] Loaded {df.shape[0]} rows x {df.shape[1]} cols")
    dq = run_dq_checks(df, "NETFLIX")

    # Silver — fill nulls with 0, parse dates, extract country
    df = df.fillna(0)
    df.columns = df.columns.str.lower().str.strip()
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['added_year'] = df['date_added'].dt.year.fillna(0).astype(int)
    df['added_month'] = df['date_added'].dt.month.fillna(0).astype(int)
    df['primary_country'] = df['country'].astype(str).str.split(',').str[0].str.strip()
    df.loc[df['primary_country'] == '0', 'primary_country'] = 'Unknown'
    os.makedirs(silver_dir, exist_ok=True)
    df.to_csv(f"{silver_dir}/netflix_clean.csv", index=False)
    logger.info(f"[SILVER] Saved {df.shape[0]} rows → {silver_dir}/netflix_clean.csv")

    # Gold — aggregations
    os.makedirs(gold_dir, exist_ok=True)
    agg = df.groupby(['type', 'release_year']).size().reset_index(name='count')
    agg.to_csv(f"{gold_dir}/netflix_aggregated.csv", index=False)
    country_stats = df['primary_country'].value_counts().reset_index()
    country_stats.columns = ['country', 'title_count']
    country_stats.to_csv(f"{gold_dir}/netflix_country_stats.csv", index=False)
    logger.info(f"[GOLD] Saved aggregated ({agg.shape[0]} rows) + country_stats ({country_stats.shape[0]} rows)")
    logger.info("NETFLIX PIPELINE COMPLETED ✓")

    return {"dataset": "netflix", "rows": len(df), "dq": dq}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Kaggle ETL Pipeline")
    parser.add_argument("--bronze-path", default="data/bronze", help="Path to bronze (raw) CSV files")
    parser.add_argument("--output-path", default="data", help="Base output path for silver/gold layers")
    args = parser.parse_args()

    silver = os.path.join(args.output_path, "silver")
    gold = os.path.join(args.output_path, "gold")

    logger.info("=" * 60)
    logger.info(f"ALL PIPELINES STARTED — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    results = []
    pipelines = [
        ("Titanic",      run_titanic,      f"{args.bronze_path}/titanic.csv"),
        ("House Prices", run_house_prices, f"{args.bronze_path}/house_prices.csv"),
        ("Netflix",      run_netflix,      f"{args.bronze_path}/netflix_titles.csv"),
    ]

    for name, fn, path in pipelines:
        try:
            result = fn(path, silver, gold)
            result['status'] = "✓ SUCCESS"
            results.append(result)
        except FileNotFoundError:
            logger.warning(f"{name}: source file not found at {path} — skipped")
            results.append({"dataset": name.lower(), "status": "⚠ SKIPPED"})
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            results.append({"dataset": name.lower(), "status": f"✗ FAILED: {e}"})

    # Save report
    os.makedirs("reports", exist_ok=True)
    report_path = f"reports/pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for r in results:
        logger.info(f"  {r['dataset']:<15} {r.get('status',''):<15} rows: {r.get('rows','N/A')}")
    logger.info("=" * 60)
    logger.info(f"Report saved: {report_path}")


if __name__ == "__main__":
    main()
