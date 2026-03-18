# Batch Data Pipeline for Sales Analytics

A production-grade batch ETL pipeline that ingests raw Superstore sales data, transforms it into a star schema, and serves aggregated KPIs to BI tools — with **daily refresh** via Apache Airflow.

---

## Architecture

```
Raw CSV (superstore.csv)
       │
       ▼
 [1] check_file          — Validate file exists + schema check
       │
       ▼
 [2] load_to_staging     — Truncate + bulk load → stg_superstore
       │
       ▼
 [3] data_validation     — Null %, duplicates, constraints
       │
       ▼
 [4] transform_data      — Clean + derive columns → fact/dim tables
       │
       ▼
 [5] build_aggregates    — Refresh agg_sales_summary (KPI table)
       │
       ▼
  PostgreSQL (sales_dw)  →  Power BI / BI Tools
```

---

## Tech Stack

| Layer          | Technology              |
|----------------|------------------------|
| Language        | Python 3.10+           |
| Orchestration   | Apache Airflow 2.8.1   |
| Processing      | Pandas                 |
| Storage         | PostgreSQL 15          |
| Visualization   | Power BI               |
| Containerization| Docker + Docker Compose |

---

## Project Structure

```
├── dags/
│   └── sales_pipeline.py       # Airflow DAG (5 tasks, @daily)
├── scripts/
│   ├── ingestion.py            # File check + staging load
│   ├── validate.py             # Data quality checks
│   ├── transform.py            # Clean + load star schema
│   └── build_aggregates.py     # Refresh KPI aggregation table
├── sql/
│   ├── 01_create_staging.sql   # stg_superstore DDL
│   └── 02_create_warehouse.sql # fact/dim/agg table DDLs
├── utils/
│   ├── db.py                   # DB connection + SQL helpers
│   └── logger.py               # Logging utility
├── config/
│   └── config.yaml             # Pipeline configuration
├── tests/
│   └── test_pipeline.py        # Unit tests (pytest)
├── data/                       # Place superstore.csv here
├── logs/                       # Pipeline logs (gitignored)
├── docker-compose.yml          # Full stack: Postgres + Airflow
└── requirements.txt
```

---

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose installed
- Superstore CSV from Kaggle placed in `data/superstore.csv`

### 2. Start the Stack

```bash
docker-compose up -d
```

Services started:
- PostgreSQL on `localhost:5432`
- Airflow Webserver on `http://localhost:8080`
- Airflow Scheduler (background)

Default Airflow credentials: `admin / admin`

### 3. Trigger the Pipeline

Either wait for the `@daily` schedule or trigger manually:

```bash
# Via Airflow UI → DAGs → sales_pipeline → Trigger DAG
# Or via CLI inside the scheduler container:
docker exec -it <scheduler-container> airflow dags trigger sales_pipeline
```

### 4. Run Locally (without Docker)

```bash
pip install -r requirements.txt

# Set environment variables or update config/config.yaml
export DB_HOST=localhost DB_PORT=5432 DB_NAME=sales_dw DB_USER=airflow DB_PASSWORD=airflow

# Run each step individually
python scripts/ingestion.py
python scripts/validate.py
python scripts/transform.py
python scripts/build_aggregates.py
```

### 5. Run Tests

```bash
pip install pytest
pytest tests/ -v
```

---

## Data Model

### Staging
| Table            | Description              |
|------------------|--------------------------|
| `stg_superstore` | Raw CSV data, truncated daily |

### Warehouse (Star Schema)
| Table              | Type      | Description                      |
|--------------------|-----------|----------------------------------|
| `fact_sales`       | Fact      | Order-level transactions         |
| `dim_customer`     | Dimension | Customer master                  |
| `dim_product`      | Dimension | Product master with category     |
| `dim_region`       | Dimension | Region reference                 |
| `dim_date`         | Dimension | Date calendar table              |
| `agg_sales_summary`| Aggregate | KPIs by date × region × category |

### Key KPIs in `agg_sales_summary`
| Metric         | Formula              |
|----------------|----------------------|
| `total_sales`  | SUM(sales)           |
| `total_profit` | SUM(profit)          |
| `total_orders` | COUNT(DISTINCT order_id) |
| `avg_discount` | AVG(discount)        |

---

## Data Quality Rules

| Check                    | Rule                          | Action on Fail |
|--------------------------|-------------------------------|----------------|
| File exists              | Path must exist               | Raise error    |
| Schema match             | All required columns present  | Raise error    |
| Row count                | > 0 rows                      | Raise error    |
| Null % (critical cols)   | < 5%                          | Raise error    |
| Duplicate orders         | (order_id, product_id) unique | Warning + dedup|
| Sales constraint         | sales >= 0                    | Raise error    |

---

## Configuration

Edit `config/config.yaml` or set environment variables:

| Key          | Env Var       | Default       |
|--------------|--------------|---------------|
| DB host      | `DB_HOST`    | `localhost`   |
| DB port      | `DB_PORT`    | `5432`        |
| DB name      | `DB_NAME`    | `sales_dw`    |
| DB user      | `DB_USER`    | `airflow`     |
| DB password  | `DB_PASSWORD`| `airflow`     |
| Data dir     | —            | `/opt/airflow/data` |
| Source file  | —            | `superstore.csv` |

---

## Non-Functional Targets

| Requirement       | Target                        |
|-------------------|-------------------------------|
| Pipeline runtime  | < 10 minutes                  |
| Data freshness    | Daily                         |
| Failure recovery  | Auto-retry (2 attempts, 5 min)|
| Logging           | Task-level logs in Airflow    |
| Query latency     | < 2 seconds (PostgreSQL)      |
