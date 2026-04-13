"""
airflow_dag.py — Superstore Sales Batch ETL Pipeline
Schedule: Daily (@daily)
PRD: etl_project_requirements.md
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

DATA_PATH = "data/superstore.csv"
TRANSFORMED_DIR = "data/transformed"
DQ_SCORECARD_PATH = "data/dq_scorecard.json"

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def check_file(**kwargs):
    """Task 1: Validate source file exists and is readable."""
    import os
    import pandas as pd

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Source file missing: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH, nrows=5)
    required_cols = [
        "order_id", "order_date", "ship_date", "customer_id",
        "region", "product_id", "sales", "quantity", "discount", "profit"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Schema mismatch — missing columns: {missing}")

    print(f"[CHECK_FILE] File OK — path={DATA_PATH}, schema validated")


def load_to_staging(**kwargs):
    """Task 2: Load raw CSV into staging representation."""
    import pandas as pd
    import os

    df = pd.read_csv(DATA_PATH)
    row_count = len(df)

    # Simulate staging write — in production this would INSERT INTO stg_superstore
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    df.to_csv(f"{TRANSFORMED_DIR}/stg_superstore.csv", index=False)

    print(f"[STAGING] Loaded {row_count} rows → stg_superstore")
    kwargs["ti"].xcom_push(key="staging_row_count", value=row_count)


def data_validation(**kwargs):
    """Task 3: Run data quality checks on staged data."""
    from pipeline.dq_checks import run_dq_checks

    scorecard = run_dq_checks(DATA_PATH, DQ_SCORECARD_PATH)

    print(f"[DQ] Score: {scorecard['dq_score_pct']}% — Status: {scorecard['status']}")
    for check in scorecard["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        print(f"  [{status}] {check['check']}: {check['detail']}")

    # Non-blocking — log failures but do not halt pipeline
    failed = [c for c in scorecard["checks"] if not c["passed"]]
    if failed:
        print(f"[DQ WARNING] {len(failed)} check(s) failed — review scorecard")


def transform_data(**kwargs):
    """Task 4: Apply cleaning, derive columns, build star schema tables."""
    from pipeline.transform import run_transform

    tables = run_transform(DATA_PATH, TRANSFORMED_DIR)
    summary = {name: len(tbl) for name, tbl in tables.items()}
    print(f"[TRANSFORM] Tables built: {summary}")
    kwargs["ti"].xcom_push(key="transform_summary", value=summary)


def load_fact_dim(**kwargs):
    """Task 5: Load fact and dimension tables (simulate DB load)."""
    import pandas as pd
    import os

    tables = [
        "fact_sales", "dim_customer", "dim_product",
        "dim_region", "dim_date"
    ]
    for table in tables:
        path = os.path.join(TRANSFORMED_DIR, f"{table}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            # In production: df.to_sql(table, engine, if_exists='replace')
            print(f"[LOAD] {table}: {len(df)} rows loaded")
        else:
            print(f"[LOAD WARNING] {table}.csv not found — skipping")


def build_aggregates(**kwargs):
    """Task 6: Build agg_sales_summary table."""
    import pandas as pd
    import os

    agg_path = os.path.join(TRANSFORMED_DIR, "agg_sales_summary.csv")
    if os.path.exists(agg_path):
        df = pd.read_csv(agg_path)
        total_sales = round(df["total_sales"].sum(), 2)
        total_orders = int(df["total_orders"].sum())
        print(f"[AGG] agg_sales_summary: {len(df)} rows — "
              f"Total Sales: ${total_sales:,.2f} | Total Orders: {total_orders:,}")
    else:
        print("[AGG WARNING] agg_sales_summary.csv not found")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="superstore_sales_pipeline",
    default_args=default_args,
    description="Batch ETL pipeline for Superstore Sales Analytics",
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "etl", "batch"],
) as dag:

    t1_check_file = PythonOperator(
        task_id="check_file",
        python_callable=check_file,
    )

    t2_load_staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    t3_data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=data_validation,
    )

    t4_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t5_load_fact_dim = PythonOperator(
        task_id="load_fact_dim",
        python_callable=load_fact_dim,
    )

    t6_build_aggregates = PythonOperator(
        task_id="build_aggregates",
        python_callable=build_aggregates,
    )

    # DAG dependency chain per PRD §7
    t1_check_file >> t2_load_staging >> t3_data_validation >> t4_transform >> t5_load_fact_dim >> t6_build_aggregates
