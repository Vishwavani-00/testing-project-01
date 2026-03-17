# Kaggle Data Engineering Pipelines

A scalable, modular ETL pipeline system for 5 Kaggle datasets — built for analytics, dashboards, and ML use cases.

## Architecture (Bronze → Silver → Gold)

```
Raw CSVs (Bronze) → Cleaned Data (Silver) → Aggregated/Business-Ready (Gold)
```

## Datasets Covered

| # | Dataset | Use Case | Pipeline |
|---|---------|----------|----------|
| 1 | Titanic | Survival prediction | `pipelines/titanic_pipeline.py` |
| 2 | House Prices | Price prediction | `pipelines/house_prices_pipeline.py` |
| 3 | Netflix Movies | Content analytics | `pipelines/netflix_pipeline.py` |
| 4 | Credit Card Fraud | Anomaly detection | `pipelines/fraud_pipeline.py` |
| 5 | Retail Sales | Sales forecasting | `pipelines/retail_sales_pipeline.py` |

## Project Structure

```
kaggle-de-project/
├── pipelines/              # Dataset-specific ETL pipelines
│   ├── titanic_pipeline.py
│   ├── house_prices_pipeline.py
│   ├── netflix_pipeline.py
│   ├── fraud_pipeline.py
│   └── retail_sales_pipeline.py
├── utils/
│   ├── data_quality.py     # Reusable DQ checks
│   ├── transformations.py  # Common transformation logic
│   └── logger.py           # Logging utility
├── config/
│   └── pipeline_config.yaml  # Centralized config
├── dags/
│   └── kaggle_pipeline_dag.py  # Airflow DAG
├── data/
│   ├── bronze/             # Raw ingested data
│   ├── silver/             # Cleaned data
│   └── gold/               # Aggregated, business-ready
├── tests/
│   └── test_pipelines.py   # Unit tests
├── requirements.txt
└── README.md
```

## Setup

```bash
pip install -r requirements.txt
```

## Running Pipelines

```bash
# Run individual pipeline
python pipelines/titanic_pipeline.py

# Run all pipelines
python run_all.py

# Via Airflow
airflow dags trigger kaggle_de_pipeline
```

## Success Metrics
- Pipeline success rate > 95%
- Data latency < 1 hour
- Dashboard refresh time < 5 mins
