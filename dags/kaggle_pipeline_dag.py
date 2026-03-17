"""
Airflow DAG — Kaggle Data Engineering Pipelines
Orchestrates all 5 pipelines in sequence with dependency management.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.titanic_pipeline import run as run_titanic
from pipelines.house_prices_pipeline import run as run_house_prices
from pipelines.netflix_pipeline import run as run_netflix
from pipelines.fraud_pipeline import run as run_fraud
from pipelines.retail_sales_pipeline import run as run_retail_sales

default_args = {
    "owner": "ved-musigma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kaggle_de_pipeline",
    default_args=default_args,
    description="Kaggle Data Engineering Pipelines — 5 datasets, Bronze to Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "kaggle", "musigma"],
) as dag:

    titanic_task = PythonOperator(
        task_id="titanic_pipeline",
        python_callable=run_titanic,
    )

    house_prices_task = PythonOperator(
        task_id="house_prices_pipeline",
        python_callable=run_house_prices,
    )

    netflix_task = PythonOperator(
        task_id="netflix_pipeline",
        python_callable=run_netflix,
    )

    fraud_task = PythonOperator(
        task_id="fraud_pipeline",
        python_callable=run_fraud,
    )

    retail_sales_task = PythonOperator(
        task_id="retail_sales_pipeline",
        python_callable=run_retail_sales,
    )

    # All pipelines run in parallel (independent datasets)
    [titanic_task, house_prices_task, netflix_task, fraud_task, retail_sales_task]
