# Superstore Sales Batch ETL Pipeline

A production-grade batch data pipeline for Sales Analytics built on the Kaggle Superstore Sales Dataset.

## Architecture

```
Raw CSV → Staging Layer → Transform Layer → Star Schema (PostgreSQL) → BI Layer
```

## Pipeline Components

| File | Description |
|---|---|
| `pipeline/airflow_dag.py` | Airflow DAG — 6-task daily pipeline |
| `pipeline/transform.py` | ETL transformation — cleaning, star schema, aggregations |
| `pipeline/dq_checks.py` | Data quality validation — nulls, duplicates, schema, constraints |
| `sql/schema.sql` | PostgreSQL DDL — all tables, indexes, and views |
| `data/superstore.csv` | Superstore Sales dataset (9,994 rows) |

## DAG Flow

```
check_file → load_to_staging → data_validation → transform_data → load_fact_dim → build_aggregates
```

**Schedule:** Daily (`@daily`) | **Retries:** 2 | **Retry delay:** 5 min

## Data Model

### Fact Table
- `fact_sales` — 9,994 rows — sales transactions with derived profit_margin, order_month, order_year

### Dimension Tables
- `dim_customer` — 794 unique customers
- `dim_product` — 7,227 unique products
- `dim_region` — 4 regions
- `dim_date` — 1,096 unique dates

### Aggregated Table
- `agg_sales_summary` — 6,971 rows — grouped by date + region + category

## Data Quality Rules

| Check | Threshold |
|---|---|
| Null % per column | < 5% |
| Duplicates (order_id + product_id) | 0 |
| Row count | >= 100 |
| Sales values | >= 0 |

**DQ Score (latest run): 100% — PASS ✅**

## Setup

```bash
pip install -r requirements.txt
# Create PostgreSQL schema
psql -U postgres -d your_db -f sql/schema.sql
# Run pipeline
python3 pipeline/transform.py
python3 pipeline/dq_checks.py
```

## CI/CD

GitHub Actions runs on every push:
1. `refresh-data` — DQ checks + transform validation
2. `lint` — flake8 on all pipeline files
3. `validate` — end-to-end pipeline logic test

Scheduled daily at **6:00 AM UTC** via cron.

## Key Business Questions Answered

- Daily/Monthly revenue trends → `v_monthly_revenue` view
- Top-performing products → `v_top_products` view
- Region-wise sales performance → `v_region_performance` view
- Customer purchase behavior → `dim_customer` + `fact_sales` joins
